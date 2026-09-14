# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-002-LLMAnalyzer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Classifies each detected failure using the Databricks-hosted model serving endpoint.
# MAGIC Uses the same OpenAI SDK pattern as `Manual_File_Agent.py` — no external credentials,
# MAGIC no GitHub PAT. Auth uses the cluster's own Databricks token, auto-available in any notebook.
# MAGIC
# MAGIC ### Inputs (set by 000-Master / 001-FailureDetector before `%run`)
# MAGIC
# MAGIC | Variable | Description |
# MAGIC |:--------:|:-----------:|
# MAGIC | `policy` | `PipelineMonitor_policy.json` as a Python dict |
# MAGIC | `detected_failures` | List of failure dicts from 001-FailureDetector |
# MAGIC
# MAGIC ### Output
# MAGIC - `analyzed_failures` — `detected_failures` enriched with: `failure_type`, `confidence`, `recommended_action`, `rationale`
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-11 | Mosaic Team | Initial implementation — Databricks model serving | Feature |

# COMMAND ----------

import json
import re
import os
from openai import OpenAI

# COMMAND ----------

# Connect to Databricks-hosted model serving — same pattern as Manual_File_Agent.py
# Token is pulled from the cluster context automatically; no secret needed.

_DB_HOST    = spark.conf.get("spark.databricks.workspaceUrl")
_DB_BASE_URL = f"https://{_DB_HOST}/serving-endpoints"
_LLM_MODEL  = policy.get("llm_model", "databricks-gpt-5-2")

try:
    _token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
except Exception:
    _token = os.getenv("DATABRICKS_TOKEN", "")

_llm_client = OpenAI(api_key=_token, base_url=_DB_BASE_URL) if _token else None

_SYSTEM_PROMPT = """You are a Databricks pipeline failure analyst.
Analyse the provided job failure context and return a JSON object with exactly these keys:
- failure_type: one of TRANSIENT, DATA_ERROR, CONFIG_ERROR, UNKNOWN
- confidence: float between 0.0 and 1.0
- recommended_action: one of RERUN, ALERT_ONLY
- rationale: one concise sentence explaining the classification
- code_fix_description: concise, actionable fix text for NotebookPatcher, or null if no code edit is needed

Classification rules:
TRANSIENT    = cluster terminated unexpectedly, timeout, OOM, driver restart, spark context lost
               → recommended_action: RERUN  (high confidence)
DATA_ERROR   = schema mismatch, null constraint, column not found, data quality failure
               → recommended_action: ALERT_ONLY
CONFIG_ERROR = missing table, permission denied, secret not found, invalid parameter
               → recommended_action: ALERT_ONLY
UNKNOWN      = cannot determine from available information
               → recommended_action: ALERT_ONLY

code_fix_description rules:
- Populate it only when failure_type is DATA_ERROR or CONFIG_ERROR and the issue needs a code-level fix.
- Keep it short and directly actionable.
- For undefined variable errors, tell NotebookPatcher what variable initialisation or fallback to add.
- Use null for TRANSIENT or UNKNOWN.


Respond with valid JSON only. No markdown fences, no explanation outside the JSON."""

# COMMAND ----------

def _derive_code_fix_description(error_message, failure_type, rationale):
    """Create a short patch instruction when the LLM does not return one."""
    if failure_type not in ("DATA_ERROR", "CONFIG_ERROR"):
        return None

    message = "" if error_message is None else str(error_message)
    message_lower = message.lower()

    name_error_match = re.search(r"name '([^']+)' is not defined", message, re.IGNORECASE)
    if name_error_match:
        variable_name = name_error_match.group(1)
        return (
            f"Initialize {variable_name} before first use, preferably from workflow parameters or a safe default, "
            f"so the notebook does not raise NameError."
        )

    if "not defined" in message_lower:
        return (
            "Initialize the missing variable before first use, sourcing it from workflow parameters or a safe default."
        )

    if "missing" in message_lower and "parameter" in message_lower:
        return (
            "Read the missing parameter from workflow inputs before it is referenced, and fail fast with a clear error if it is absent."
        )

    if "permission" in message_lower:
        return (
            "Use the configured secret or connection setting before the failing access, and surface a clear configuration error if permissions are missing."
        )

    if "table" in message_lower and ("not found" in message_lower or "does not exist" in message_lower):
        return (
            "Verify the source table name is resolved from configuration before use, and guard the lookup with a clear missing-table check."
        )

    return rationale if rationale else "Apply the minimal code/configuration fix needed to resolve the failure."


def classify_failure(job_name, termination_code, error_message):
    """
    Sends failure context to the Databricks-hosted LLM and returns a classification dict.
    Falls back to UNKNOWN with confidence 0.0 on any API or parse error.
    """
    if not _llm_client:
        print(f"[LLMAnalyzer] WARNING: No LLM client for '{job_name}'. Defaulting to UNKNOWN.")
        return {
            "failure_type": "UNKNOWN",
            "confidence": 0.0,
            "recommended_action": "ALERT_ONLY",
            "rationale": "LLM client not initialised — token unavailable.",
            "code_fix_description": None
        }

    user_message = (
        f"Job name: {job_name}\n"
        f"Termination code: {termination_code}\n"
        f"Error message: {error_message}\n\n"
        "Classify this failure."
    )

    try:
        response = _llm_client.chat.completions.create(
            model=_LLM_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_message}
            ],
            max_tokens=300,
            temperature=0,
            top_p=0.3
        )

        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        result = json.loads(raw)

        required_keys = {"failure_type", "confidence", "recommended_action", "rationale"}
        missing = required_keys - result.keys()
        if missing:
            raise ValueError(f"LLM response missing keys: {missing}")

        result["confidence"] = float(result["confidence"])

        if "code_fix_description" not in result:
            result["code_fix_description"] = None
        else:
            result["code_fix_description"] = (
                str(result["code_fix_description"]).strip() or None
                if result["code_fix_description"] is not None
                else None
            )

        if result["failure_type"] not in ("DATA_ERROR", "CONFIG_ERROR"):
            result["code_fix_description"] = None
        elif not result["code_fix_description"]:
            result["code_fix_description"] = _derive_code_fix_description(
                error_message=error_message,
                failure_type=result["failure_type"],
                rationale=result.get("rationale", "")
            )

        if not isinstance(result, dict):
            raise TypeError(f"LLM classification did not return a mapping: {type(result)!r}")

        return result

    except Exception as e:
        print(f"[LLMAnalyzer] WARNING: classification failed for '{job_name}': {e}")
        return {
            "failure_type": "UNKNOWN",
            "confidence": 0.0,
            "recommended_action": "ALERT_ONLY",
            "rationale": f"LLM classification unavailable: {str(e)}",
            "code_fix_description": None
        }

# COMMAND ----------

analyzed_failures = []

for failure in detected_failures:
    classification = classify_failure(
        job_name=failure["job_name"],
        termination_code=failure.get("termination_code", ""),
        error_message=failure.get("error_message", "")
    )
    analyzed_failures.append({**failure, **classification})
    print(
        f"[LLMAnalyzer] {failure['job_name']}: "
        f"{classification['failure_type']} (confidence={classification['confidence']:.2f}) "
        f"→ {classification['recommended_action']}"
    )
    print(f"              {classification['rationale']}")