# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-006-NotebookPatcher
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Databricks-native equivalent of the VS Code `@NotebookEditor` agent.
# MAGIC For failures classified as `DATA_ERROR` or `CONFIG_ERROR` where the LLM identified a specific
# MAGIC code-level fix (`code_fix_description`), this notebook:
# MAGIC
# MAGIC 1. Resolves the failing notebook's repo path from the job task settings (via SDK)
# MAGIC 2. Fetches the notebook `.py` source from Azure DevOps via REST API
# MAGIC 3. Calls the Databricks LLM to apply the targeted fix (second, focused LLM call)
# MAGIC 4. Creates a branch + commits the patched file + opens a PR via ADO REST API
# MAGIC 5. Logs the PR URL to `patch_results` (picked up by 005-AuditLogger)
# MAGIC
# MAGIC ### Architecture
# MAGIC
# MAGIC ```
# MAGIC execution_results (from 004)
# MAGIC   │  filter: failure_type in (DATA_ERROR, CONFIG_ERROR)
# MAGIC   │          AND code_fix_description is not null
# MAGIC   ▼
# MAGIC _get_task_notebook_path()   → w.jobs.get() → task.notebook_task.notebook_path
# MAGIC _fetch_file_from_ado()      → ADO REST GET /items
# MAGIC _call_llm_for_patch()       → LLM: "apply this fix to this source"  (max_tokens=8192)
# MAGIC _create_pr_branch_commit()  → ADO REST POST /pushes + /pullrequests
# MAGIC   │
# MAGIC   ▼
# MAGIC patch_results[]  →  PR URL logged in AgentActionLog by 005-AuditLogger
# MAGIC ```
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC | Requirement | Detail |
# MAGIC |:-----------:|:------:|
# MAGIC | `notebook_patch_enabled: true` | In `PipelineMonitor_policy.json` |
# MAGIC | `ado_org`, `ado_project`, `ado_repo` | ADO coordinates in policy |
# MAGIC | ADO PAT in Databricks secret | Scope/key set via `ado_secret_scope` / `ado_secret_key` |
# MAGIC | PAT permissions | `Code (Read & Write)` + `Pull Request (Contribute)` |
# MAGIC
# MAGIC ### Inputs (set by 000-Master before `%run`)
# MAGIC
# MAGIC | Variable | Source |
# MAGIC |:--------:|:------:|
# MAGIC | `policy` | `PipelineMonitor_policy.json` |
# MAGIC | `execution_results` | 004-ActionExecutor |
# MAGIC | `dry_run` | widget |
# MAGIC | `_llm_client`, `_LLM_MODEL` | 002-LLMAnalyzer shared scope |
# MAGIC
# MAGIC ### Output
# MAGIC - `patch_results` — list of dicts: `{**failure, patch_status, pr_url, branch_name, repo_path}`
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-19 | Mosaic Team | Initial implementation — Databricks-native notebook editor agent | Feature |

# COMMAND ----------

import base64
import re
import requests

# COMMAND ----------

_PATCH_ENABLED    = bool(policy.get("notebook_patch_enabled", False))
_ADO_ORG          = policy.get("ado_org", "")
_ADO_PROJECT      = policy.get("ado_project", "")
_ADO_REPO         = policy.get("ado_repo", "cdo_mosaic")
_ADO_BASE_BRANCH  = policy.get("ado_base_branch", "main")
_ADO_SECRET_SCOPE = policy.get("ado_secret_scope", "")
_ADO_SECRET_KEY   = policy.get("ado_secret_key", "")
_PATH_PREFIX_DB   = policy.get("notebook_path_prefix_db",   "/Mosaic/")
_PATH_PREFIX_REPO = policy.get("notebook_path_prefix_repo", "databricks/")

# COMMAND ----------

if not _PATCH_ENABLED:
    print("[NotebookPatcher] notebook_patch_enabled=false in policy — skipping patch step")
    patch_results = []

elif not all([_ADO_ORG, _ADO_PROJECT, _ADO_REPO, _ADO_SECRET_SCOPE, _ADO_SECRET_KEY]):
    print("[NotebookPatcher] WARNING: ADO config incomplete in policy — skipping patch step")
    print(f"[NotebookPatcher] Missing: ado_org={_ADO_ORG!r}  ado_project={_ADO_PROJECT!r}  "
          f"ado_secret_scope={_ADO_SECRET_SCOPE!r}  ado_secret_key={_ADO_SECRET_KEY!r}")
    patch_results = []

else:
    _ADO_PAT     = dbutils.secrets.get(scope=_ADO_SECRET_SCOPE, key=_ADO_SECRET_KEY)
    _ADO_AUTH    = base64.b64encode(f":{_ADO_PAT}".encode()).decode()
    _ADO_HEADERS = {"Authorization": f"Basic {_ADO_AUTH}", "Content-Type": "application/json"}
    _ADO_BASE    = (
        f"https://dev.azure.com/{_ADO_ORG}/{_ADO_PROJECT}"
        f"/_apis/git/repositories/{_ADO_REPO}"
    )

    # COMMAND ----------

    def _notebook_db_path_to_repo_path(db_path):
        """
        Maps a Databricks workspace path to the repo-relative file path.
        e.g. /Mosaic/PBNA/Topline/Gold/NR_Enhanced/001-ETL
             → databricks/PBNA/Topline/Gold/NR_Enhanced/001-ETL.py
        """
        path = db_path or ""
        if path.startswith(_PATH_PREFIX_DB):
            path = _PATH_PREFIX_REPO + path[len(_PATH_PREFIX_DB):]
        if not path.endswith(".py"):
            path += ".py"
        return path

    def _get_task_notebook_path(job_id, task_key):
        """
        Resolves the notebook path for a specific task key from the job definition.
        Falls back to the first notebook task found if task_key doesn't match.
        """
        from databricks.sdk import WorkspaceClient
        _w = WorkspaceClient()
        job  = _w.jobs.get(job_id=int(job_id))
        tasks = job.settings.tasks or []

        # Try exact task_key match first
        for task in tasks:
            if task.task_key == task_key and getattr(task, "notebook_task", None):
                return task.notebook_task.notebook_path

        # Fall back to first notebook task
        for task in tasks:
            if getattr(task, "notebook_task", None):
                return task.notebook_task.notebook_path

        return None

    # COMMAND ----------

    def _fetch_file_and_base_commit(repo_file_path):
        """
        Fetches the raw file content and the HEAD commit SHA of the base branch from ADO.
        Returns (file_content: str, base_commit_sha: str).
        """
        # File content
        item_url = f"{_ADO_BASE}/items?path={repo_file_path}&api-version=7.0"
        item_resp = requests.get(item_url, headers=_ADO_HEADERS, timeout=30)
        item_resp.raise_for_status()
        file_content = item_resp.text

        # Base branch HEAD commit SHA
        refs_url = f"{_ADO_BASE}/refs?filter=heads/{_ADO_BASE_BRANCH}&api-version=7.0"
        refs_resp = requests.get(refs_url, headers=_ADO_HEADERS, timeout=30)
        refs_resp.raise_for_status()
        refs_values = refs_resp.json().get("value", [])
        if not refs_values:
            raise ValueError(f"Branch '{_ADO_BASE_BRANCH}' not found in ADO repo")
        base_commit_sha = refs_values[0]["objectId"]

        return file_content, base_commit_sha

    # COMMAND ----------

    _PATCHER_SYSTEM_PROMPT = """You are a Databricks PySpark notebook code editor.

Apply a targeted, minimal fix to the notebook Python source provided.

STRICT RULES:
- Make ONLY the change described in the fix instruction. Do not refactor or improve anything else.
- Preserve all Databricks cell markers: # COMMAND ----------, # MAGIC %md, # MAGIC %run, # MAGIC %pip
- Preserve all comments, blank lines, and indentation exactly as they are outside the changed area.
- Do NOT add docstrings, explanations, or markdown comments to the output.
- Return ONLY the corrected Python source. No markdown fences. No preamble. No explanation."""

    _WINDOW_CHARS = 14_000   # characters per context window sent to the patching LLM
    _OVERLAP_CHARS = 2_000   # overlap between windows to avoid cutting across cell boundaries

    def _extract_relevant_window(notebook_source, error_message, fix_description, window=14000, overlap=2000):
        """
        For long notebooks, extract the most relevant slice of source rather than blindly
        truncating from the start (which misses write/schema sections at the bottom).

        Strategy (in priority order):
          1. Find the cell containing the error keyword(s) mentioned in the error message.
          2. Find cells with write operations (saveAsTable, write.format, delta, MERGE).
          3. Fall back to: first 4000 chars (imports/widgets) + last 8000 chars (business logic + writes).

        Returns a tuple (windowed_source: str, was_truncated: bool).
        """
        if len(notebook_source) <= window:
            return notebook_source, False

        cells = notebook_source.split("# COMMAND ----------")
        if not cells:
            return notebook_source[:window], True

        # Extract keywords from the error message to find the relevant cell
        error_keywords = re.findall(r"['\"`]([a-zA-Z_][a-zA-Z0-9_]{2,})['\"`]", error_message)
        fix_keywords   = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]{3,})\b", fix_description)
        search_terms   = set(k.lower() for k in error_keywords + fix_keywords)

        write_patterns = re.compile(
            r"(saveAsTable|write\.format|\.delta|DeltaTable|MERGE INTO|INSERT INTO"
            r"|load_data_with_history|writeToSynapse|gold_tblname)",
            re.IGNORECASE
        )

        # Score each cell: +3 for error keyword match, +2 for fix keyword match, +1 for write pattern
        scored = []
        for i, cell in enumerate(cells):
            cell_lower = cell.lower()
            score  = sum(3 for k in error_keywords if k.lower() in cell_lower)
            score += sum(2 for k in fix_keywords   if k.lower() in cell_lower)
            score += 2 if write_patterns.search(cell) else 0
            scored.append((score, i))

        scored.sort(key=lambda x: -x[0])
        top_indices = set(idx for _, idx in scored[:5] if scored[0][0] > 0)

        # Always include the first 2 cells (imports, widgets) and last 2 cells (writes, cleanup)
        top_indices |= {0, 1, len(cells) - 2, len(cells) - 1}
        top_indices  = sorted(i for i in top_indices if 0 <= i < len(cells))

        windowed = "# COMMAND ----------".join(cells[i] for i in top_indices)

        if len(windowed) > window:
            windowed = windowed[:window]

        omitted = len(cells) - len(top_indices)
        if omitted > 0:
            windowed = (
                f"# NOTE: {omitted} cells omitted for context budget. "
                f"Showing cells most relevant to the fix.\n\n" + windowed
            )

        return windowed, True


    def _call_llm_for_patch(repo_path, notebook_source, error_message, code_fix_description):
        """
        Second LLM call: applies a code-level fix to the notebook source.
        Uses smart windowing to keep context within token budget while prioritising
        the cells most likely to contain the fix location.
        Returns the patched source string.
        """
        windowed_source, was_truncated = _extract_relevant_window(
            notebook_source, error_message, code_fix_description
        )
        if was_truncated:
            print(f"[NotebookPatcher]   Context windowed: {len(windowed_source)}/{len(notebook_source)} chars sent to LLM")

        user_message = (
            f"Notebook: {repo_path}\n\n"
            f"Error that caused the job failure:\n{error_message[:2000]}\n\n"
            f"Fix to apply:\n{code_fix_description}\n\n"
            f"Notebook source (relevant cells):\n{windowed_source}"
        )

        response = _llm_client.chat.completions.create(
            model=_LLM_MODEL,
            messages=[
                {"role": "system", "content": _PATCHER_SYSTEM_PROMPT},
                {"role": "user",   "content": user_message}
            ],
            max_tokens=8192,
            temperature=0,
            top_p=0.1
        )
        patched = response.choices[0].message.content.strip()

        # Strip any accidental markdown fences the model may have added
        patched = re.sub(r"^```(?:python)?\s*", "", patched)
        patched = re.sub(r"\s*```$", "", patched)

        # If windowed, reconstruct: replace the windowed cells back into the full source
        # The LLM patches the windowed slice; re-merge into original to avoid losing cells
        if was_truncated and patched and patched != windowed_source:
            # Best-effort: find the changed lines and apply them back to the full source
            # Simple approach: replace each cell block that the LLM changed
            patched_cells = patched.split("# COMMAND ----------")
            source_cells  = notebook_source.split("# COMMAND ----------")
            for p_cell in patched_cells:
                p_cell_stripped = p_cell.strip()
                if not p_cell_stripped:
                    continue
                # Match by first non-empty line of the cell (cell "signature")
                p_sig = next((ln.strip() for ln in p_cell_stripped.splitlines() if ln.strip()), "")
                for j, s_cell in enumerate(source_cells):
                    s_sig = next((ln.strip() for ln in s_cell.strip().splitlines() if ln.strip()), "")
                    if p_sig and s_sig and p_sig == s_sig and p_cell.strip() != s_cell.strip():
                        source_cells[j] = p_cell
                        break
            patched = "# COMMAND ----------".join(source_cells)

        return patched

    # COMMAND ----------

    def _create_branch_commit_pr(repo_file_path, patched_content, base_commit_sha,
                                  job_name, fix_description, error_message):
        """
        Creates a new branch, commits the patched file, and opens a PR.
        Returns (pr_url: str, branch_name: str).

        ADO push API creates the branch and commits atomically.
        Branch naming: ai-self-heal/<safe-job-name>-<timestamp>
        """
        ts          = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        safe_job    = re.sub(r"[^a-zA-Z0-9_-]", "-", job_name)[:40].strip("-")
        branch_name = f"ai-self-heal/{safe_job}-{ts}"

        encoded_content = base64.b64encode(patched_content.encode("utf-8")).decode()

        push_payload = {
            "refUpdates": [{"name": f"refs/heads/{branch_name}", "oldObjectId": "0" * 40}],
            "commits": [{
                "comment": f"[AI Self-Heal] {fix_description[:120]}",
                "changes": [{
                    "changeType": "edit",
                    "item": {"path": f"/{repo_file_path}"},
                    "newContent": {"content": encoded_content, "contentType": "base64encoded"}
                }]
            }]
        }
        push_url  = f"{_ADO_BASE}/pushes?api-version=7.0"
        push_resp = requests.post(push_url, headers=_ADO_HEADERS, json=push_payload, timeout=30)
        push_resp.raise_for_status()

        pr_payload = {
            "sourceRefName": f"refs/heads/{branch_name}",
            "targetRefName": f"refs/heads/{_ADO_BASE_BRANCH}",
            "title": f"[AI Self-Heal] Fix {job_name}",
            "description": (
                f"## Automated fix raised by PipelineMonitor\n\n"
                f"**Fix applied:** {fix_description}\n\n"
                f"**Failure error (truncated):**\n```\n{error_message[:500]}\n```\n\n"
                f"> Review and merge after verifying the patch is correct."
            )
        }
        pr_url_api  = f"{_ADO_BASE}/pullrequests?api-version=7.0"
        pr_resp     = requests.post(pr_url_api, headers=_ADO_HEADERS, json=pr_payload, timeout=30)
        pr_resp.raise_for_status()

        pr_id  = pr_resp.json()["pullRequestId"]
        pr_url = (
            f"https://dev.azure.com/{_ADO_ORG}/{_ADO_PROJECT}"
            f"/_git/{_ADO_REPO}/pullrequest/{pr_id}"
        )
        return pr_url, branch_name

    # COMMAND ----------

    # Filter execution_results for patchable failures:
    #   - DATA_ERROR or CONFIG_ERROR (code-level root cause)
    #   - code_fix_description set by 002-LLMAnalyzer
    _patchable = [
        r for r in execution_results
        if r.get("code_fix_description")
        and r.get("failure_type") in ("DATA_ERROR", "CONFIG_ERROR")
    ]

    print(f"[NotebookPatcher] ── Patch step ──────────────────────────────────────────")
    print(f"[NotebookPatcher] {len(_patchable)} failure(s) eligible for notebook patch "
          f"({len(execution_results) - len(_patchable)} skipped — no code fix identified)")

    patch_results = []

    for failure in _patchable:
        _job_id               = failure["job_id"]
        _job_name             = failure["job_name"]
        _task_key             = failure.get("task_key", "")
        _code_fix_description = failure["code_fix_description"]
        _error_message        = failure.get("error_message", "")

        print(f"\n[NotebookPatcher] → {_job_name} | fix: {_code_fix_description[:80]}")

        try:
            # Step 1: Resolve notebook path from job definition
            db_path = _get_task_notebook_path(_job_id, _task_key)
            if not db_path:
                print(f"[NotebookPatcher]   SKIP — could not resolve notebook path "
                      f"(job_id={_job_id}, task_key={_task_key!r})")
                patch_results.append({**failure, "patch_status": "SKIPPED_NO_PATH",
                                       "pr_url": None, "branch_name": None, "repo_path": None})
                continue

            repo_path = _notebook_db_path_to_repo_path(db_path)
            print(f"[NotebookPatcher]   Repo path: {repo_path}")

            # Step 2: Dry run guard
            if dry_run:
                print(f"[NotebookPatcher]   DRY RUN — would fetch {repo_path}, patch, and raise PR")
                patch_results.append({**failure, "patch_status": "DRY_RUN",
                                       "pr_url": None, "branch_name": None, "repo_path": repo_path})
                continue

            # Step 3: Fetch notebook source from ADO
            notebook_source, base_commit_sha = _fetch_file_and_base_commit(repo_path)
            print(f"[NotebookPatcher]   Fetched {len(notebook_source)} chars from ADO")

            # Step 4: LLM applies the patch
            patched_source = _call_llm_for_patch(
                repo_path, notebook_source, _error_message, _code_fix_description
            )

            if not patched_source or patched_source.strip() == notebook_source.strip():
                print(f"[NotebookPatcher]   SKIP — LLM returned identical source; fix may already be applied")
                patch_results.append({**failure, "patch_status": "SKIPPED_NO_CHANGE",
                                       "pr_url": None, "branch_name": None, "repo_path": repo_path})
                continue

            # Step 5: Commit to new branch + open PR
            pr_url, branch_name = _create_branch_commit_pr(
                repo_path, patched_source, base_commit_sha,
                _job_name, _code_fix_description, _error_message
            )

            print(f"[NotebookPatcher]   PR raised: {pr_url}")
            patch_results.append({
                **failure,
                "patch_status": "PR_RAISED",
                "pr_url":       pr_url,
                "branch_name":  branch_name,
                "repo_path":    repo_path
            })

        except Exception as _e:
            print(f"[NotebookPatcher]   ERROR: {_e}")
            patch_results.append({**failure, "patch_status": f"FAILED: {str(_e)[:200]}",
                                   "pr_url": None, "branch_name": None, "repo_path": None})

    _pr_count = sum(1 for r in patch_results if r.get("patch_status") == "PR_RAISED")
    print(f"\n[NotebookPatcher] {_pr_count}/{len(_patchable)} PR(s) raised")
