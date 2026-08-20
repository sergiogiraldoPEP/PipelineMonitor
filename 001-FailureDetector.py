# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-001-FailureDetector
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Reads `system.lakeflow.job_run_timeline` and `system.lakeflow.job_task_run_timeline`
# MAGIC for failed runs in the last N minutes. Enriches each failure with job name and
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
audit_table = f"{audit_catalog}.{audit_schema}.AgentActionLog"

# COMMAND ----------
# Step 1 — Find failed runs in the lookback window, join to get failing task key

failed_runs_df = spark.sql(f"""
    SELECT
        jrt.job_id,
        jrt.run_id,
        jrt.result_state,
        jrt.termination_code,
        jrt.period_end_time,
        jrt.trigger_type,
        jrt.run_name,
        COALESCE(
            FIRST(jtrt.task_key) OVER (PARTITION BY jrt.run_id ORDER BY jtrt.period_end_time),
            'unknown_task'
        ) AS task_key
    FROM system.lakeflow.job_run_timeline jrt
    LEFT JOIN system.lakeflow.job_task_run_timeline jtrt
        ON jrt.job_id = jtrt.job_id
        AND jrt.run_id = jtrt.job_run_id
        AND jtrt.result_state IN ('FAILED', 'TIMEDOUT')
    WHERE jrt.result_state IN ('FAILED', 'TIMEDOUT')
    AND jrt.period_end_time >= NOW() - INTERVAL {lookback_min} MINUTES
""").dropDuplicates(["run_id"])

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
