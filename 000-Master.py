# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-000-Master
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Agentic self-healing agent for Databricks pipelines. Detects failed jobs from Unity Catalog
# MAGIC system tables, classifies each failure using a Databricks-hosted LLM (no external credentials),
# MAGIC applies guardrails, triggers reruns or queues human approvals, and writes a full audit trail.
# MAGIC
# MAGIC ### Pipeline Steps
# MAGIC
# MAGIC | Notebook | Description |
# MAGIC |:--------:|:-----------:|
# MAGIC | <a href="$./001-FailureDetector">001-FailureDetector</a> | Detect unhandled failures from `system.lakeflow.*` |
# MAGIC | <a href="$./002-LLMAnalyzer">002-LLMAnalyzer</a> | Classify failure type and confidence via Databricks model serving |
# MAGIC | <a href="$./003-GuardrailEngine">003-GuardrailEngine</a> | Confidence check, action tier, daily rerun cap |
# MAGIC | <a href="$./004-ActionExecutor">004-ActionExecutor</a> | Auto-rerun (SAFE) or queue for human approval (RESTRICTED) |
# MAGIC | <a href="$./005-AuditLogger">005-AuditLogger</a> | Write every decision to `AgentActionLog` (Delta upsert) |
# MAGIC
# MAGIC ### Widgets
# MAGIC
# MAGIC | Widget | Default | Description |
# MAGIC |:------:|:-------:|:-----------:|
# MAGIC | env | dev | Environment — drives config path and catalog |
# MAGIC | dry_run | True | Set False to enable live reruns |
# MAGIC | job_id | | Databricks job run ID (injected by scheduler) |
# MAGIC
# MAGIC ### One-Time Setup
# MAGIC 1. Run `scripts/agent_ddl.sql` in DBSQL to create `AgentActionLog` and `AgentApprovalQueue`
# MAGIC 2. Grant system table access: `GRANT SELECT ON SCHEMA system.lakeflow TO <cluster-principal>`
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-11 | Mosaic Team | Initial implementation — Databricks model serving, system.lakeflow | Feature |

# COMMAND ----------

# MAGIC %md ### PreMosaicExecute

# COMMAND ----------

# MAGIC %run /Mosaic/config/PreMosaicExecution

# COMMAND ----------

# MAGIC %md ### Config notebook

# COMMAND ----------

# MAGIC %run Mosaic/config/MosaicCommonParameterSetup

# COMMAND ----------

# MAGIC %md ### Imports

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

# COMMAND ----------

# MAGIC %md ### Widget parameters

# COMMAND ----------

dbutils.widgets.text("env",      "dev",  "Environment (dev / qa / prod)")
dbutils.widgets.text("dry_run",  "True", "Dry Run — True prevents any live reruns")
dbutils.widgets.text("job_id",   "",     "Job ID (injected by Databricks scheduler)")

env      = dbutils.widgets.get("env").strip().lower()
dry_run  = dbutils.widgets.get("dry_run").strip()
job_id   = dbutils.widgets.get("job_id").strip()

print(f"[PipelineMonitor] ── Cycle start ──────────────────────────────────────────")
print(f"[PipelineMonitor] env={env}  dry_run={dry_run}  time={datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# MAGIC %md ### Policy config

# COMMAND ----------

# Load PipelineMonitor_policy.json from repo configuration folder.
# Resolves the config file path from the notebook's repo location at runtime.

try:
    ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_path = ctx.notebookPath().get()
    repo_root     = "/Workspace" + "/".join(notebook_path.split("/")[:-4])
    policy_file   = f"{repo_root}/configuration/{env}/AgentConfig/PipelineMonitor_policy.json"

    with open(policy_file, "r") as _f:
        policy = json.load(_f)

    print(f"[PipelineMonitor] Policy loaded: {policy_file}")

except Exception as _e:
    error_msg = f"[PipelineMonitor] FATAL: Policy file not found at {policy_file}. Please ensure the configuration file exists. Error: {_e}"
    print(error_msg)
    raise RuntimeError(error_msg) from _e

audit_catalog = policy.get("audit_catalog")
audit_schema  = policy.get("audit_schema")
audit_table_name = policy.get("audit_table_name", "AgentActionLog")
approval_table_name = policy.get("approval_table_name", "AgentApprovalQueue")

if not audit_catalog or not audit_schema:
    raise ValueError("[PipelineMonitor] FATAL: audit_catalog and audit_schema must be defined in the policy file.")

print(f"[PipelineMonitor] Audit target: {audit_catalog}.{audit_schema}")
print(f"[PipelineMonitor] Tables: {audit_table_name}, {approval_table_name}")
print(f"[PipelineMonitor] Guardrails:   confidence≥{policy['confidence_threshold']}  max_reruns={policy['max_auto_reruns_per_job_per_day']}/day")

# COMMAND ----------

# MAGIC %md ### Step 1 — Detect failures

# COMMAND ----------

# MAGIC %run ./001-FailureDetector

# COMMAND ----------

if not detected_failures:
    print("[PipelineMonitor] ── No new failures this cycle. Exiting. ─────────────────")
    dbutils.notebook.exit("NO_FAILURES")

print(f"[PipelineMonitor] {len(detected_failures)} failure(s) to process")

# COMMAND ----------

# MAGIC %md ### Step 2 — Classify with LLM

# COMMAND ----------

# MAGIC %run ./002-LLMAnalyzer

# COMMAND ----------

# MAGIC %md ### Step 3 — Apply guardrails

# COMMAND ----------

# MAGIC %run ./003-GuardrailEngine

# COMMAND ----------

# MAGIC %md ### Step 4 — Execute or queue approvals

# COMMAND ----------

# MAGIC %run ./004-ActionExecutor

# COMMAND ----------

# MAGIC %md ### Step 5 — Write audit trail

# COMMAND ----------

# MAGIC %run ./005-AuditLogger

# COMMAND ----------

_auto    = sum(1 for r in execution_results if r.get("final_status") == "RERUN_TRIGGERED")
_pending = sum(1 for r in execution_results if r.get("final_status") == "PENDING_APPROVAL")
_blocked = sum(1 for r in execution_results if "BLOCKED" in (r.get("final_status") or ""))

print(f"[PipelineMonitor] ── Cycle complete ─────────────────────────────────────────")
print(f"[PipelineMonitor] Reruns triggered:  {_auto}")
print(f"[PipelineMonitor] Pending approval:  {_pending}")
print(f"[PipelineMonitor] Blocked:           {_blocked}")
print(f"[PipelineMonitor] Total processed:   {len(execution_results)}")
