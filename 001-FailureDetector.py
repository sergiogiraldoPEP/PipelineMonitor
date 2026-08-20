# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-001-FailureDetector
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Reads failed job runs from the Databricks Jobs API using the WorkspaceClient.
# MAGIC Filters for failed runs within the lookback window and enriches each failure with job name and
# MAGIC full error message text from the Databricks Jobs API.
# MAGIC Anti-joins against `AgentActionLog` to skip already-processed runs.
# MAGIC
# MAGIC ### Inputs (set by 000-Master before `%run`)
# MAGIC
# MAGIC | Variable | Description |
# MAGIC |:--------:|:-----------:|
# MAGIC | `policy` | `PipelineMonitor_policy.json` as a Python dict |
# MAGIC | `audit_catalog` | Unity Catalog catalog name |
# MAGIC | `audit_schema` | Schema name (e.g. `mosaic_audit`) |
# MAGIC
# MAGIC ### Output
# MAGIC - `detected_failures` — list of dicts, one per unhandled failed run; includes `country_code` extracted from job name prefix
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-11 | Mosaic Team | Initial implementation | Feature |

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone

# COMMAND ----------

w = WorkspaceClient()

lookback_min = policy.get("polling_lookback_minutes", 15)
audit_table = f"{audit_catalog}.{audit_schema}.{audit_table_name}"

# COMMAND ----------
# Step 1 — Find failed runs in the lookback window using Databricks Jobs API

from datetime import datetime, timedelta

lookback_cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_min)
failed_runs_data = []

# Fetch completed runs from Jobs API
for run in w.jobs.list_runs(completed_only=True, expand_tasks=True):
    run_end_time = run.end_time / 1000 if run.end_time else None
    
    # Filter by result state and lookback window
    if run.state and run.state.result_state:
        result_state = str(run.state.result_state.name)
        if result_state in ("FAILED", "TIMEDOUT"):
            # Check if run is within lookback window
            if run_end_time:
                run_end_dt = datetime.fromtimestamp(run_end_time, tz=timezone.utc)
                if run_end_dt >= lookback_cutoff:
                    # Extract first failed task key if available
                    task_key = "unknown_task"
                    if run.tasks:
                        for task in run.tasks:
                            if task.state and task.state.result_state:
                                task_result = str(task.state.result_state.name)
                                if task_result in ("FAILED", "TIMEDOUT"):
                                    task_key = task.task_key or "unknown_task"
                                    break
                    
                    failed_runs_data.append({
                        "job_id": run.job_id,
                        "run_id": run.run_id,
                        "result_state": result_state,
                        "termination_code": run.state.state_message or None,
                        "period_end_time": run_end_time,
                        "trigger_type": str(run.trigger.trigger_type.name) if run.trigger else "MANUAL",
                        "run_name": run.run_name or f"run_{run.run_id}",
                        "task_key": task_key
                    })

# Convert to DataFrame
failed_runs_df = spark.createDataFrame(failed_runs_data).dropDuplicates(["run_id"]) if failed_runs_data else spark.createDataFrame([], schema="job_id long, run_id long, result_state string, termination_code string, period_end_time long, trigger_type string, run_name string, task_key string")

# COMMAND ----------
# Step 2 — Anti-join against already-handled runs to avoid reprocessing

try:
    handled_df = spark.sql(f"SELECT DISTINCT RunId FROM {audit_table}")
    new_failures_df = failed_runs_df.join(
        handled_df,
        failed_runs_df.run_id == handled_df.RunId,
        "left_anti"
    )
except Exception:
    # AgentActionLog does not exist yet (first run) — treat all failures as new
    new_failures_df = failed_runs_df

new_failures = new_failures_df.collect()

# COMMAND ----------
# Step 3 — Enrich each failure with job name and error message from Jobs API

detected_failures = []

for row in new_failures:
    job_id = str(row["job_id"])
    run_id = str(row["run_id"])

    try:
        job_details = w.jobs.get(job_id=int(job_id))
        job_name = job_details.settings.name if job_details.settings else f"job_{job_id}"
    except Exception as e:
        job_name = f"job_{job_id}"

    try:
        run_details = w.jobs.runs.get(run_id=int(run_id))
        error_message = (
            run_details.state.state_message
            if run_details.state and run_details.state.state_message
            else str(row["termination_code"])
        )
    except Exception:
        error_message = str(row["termination_code"]) or "unknown error"

    # Extract country code from first 2 letters of job name
    country_code = job_name[:2].upper() if len(job_name) >= 2 else "XX"

    detected_failures.append({
        "job_id": job_id,
        "job_name": job_name,
        "country_code": country_code,
        "run_id": run_id,
        "task_key": row["task_key"],
        "result_state": row["result_state"],
        "termination_code": str(row["termination_code"]) if row["termination_code"] else None,
        "error_message": error_message,
        "detected_at": datetime.now(timezone.utc).isoformat()
    })

# COMMAND ----------

print(f"[FailureDetector] Detected {len(detected_failures)} new unhandled failure(s)")
for f in detected_failures:
    print(f"  job={f['job_name']}  run_id={f['run_id']}  termination={f['termination_code']}")
