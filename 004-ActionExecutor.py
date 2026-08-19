# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-004-ActionExecutor
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Triggers reruns for auto-approved failures, or writes approval requests for restricted ones.
# MAGIC
# MAGIC | Path | Mechanism |
# MAGIC |:----:|:---------:|
# MAGIC | Auto | `w.jobs.run_now()` via Databricks SDK — no human needed |
# MAGIC | Approval | PENDING row written to `AgentApprovalQueue`; human approves via DBSQL UPDATE |
# MAGIC | Dry run | All decisions logged, no reruns triggered, no queue rows written |
# MAGIC
# MAGIC ### Inputs (set by 000-Master / 003-GuardrailEngine before `%run`)
# MAGIC
# MAGIC | Variable | Description |
# MAGIC |:--------:|:-----------:|
# MAGIC | `policy` | `PipelineMonitor_policy.json` as a Python dict |
# MAGIC | `audit_catalog` | Unity Catalog catalog name |
# MAGIC | `audit_schema` | Schema name |
# MAGIC | `guardrail_results` | List from 003-GuardrailEngine |
# MAGIC | `dry_run` widget | `"True"` or `"False"` |
# MAGIC
# MAGIC ### Output
# MAGIC - `execution_results` — `guardrail_results` enriched with: `event_id`, `new_run_id`, `final_status`
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-11 | Mosaic Team | Initial implementation | Feature |

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone
import uuid

# COMMAND ----------

w = WorkspaceClient()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
approval_table = f"{audit_catalog}.{audit_schema}.AgentApprovalQueue"

if dry_run:
    print("[ActionExecutor] DRY RUN mode — no reruns will be triggered, no queue rows written")

# COMMAND ----------

def trigger_rerun(job_id, job_name):
    """
    Triggers a Databricks job rerun via SDK.
    Returns (new_run_id, True) on success, (None, False) on failure.
    """
    if dry_run:
        print(f"[ActionExecutor] DRY RUN — skipping rerun for job_id={job_id} ({job_name})")
        return "DRY_RUN_SKIPPED", True

    try:
        run_response = w.jobs.run_now(job_id=int(job_id))
        new_run_id = str(run_response.run_id)
        print(f"[ActionExecutor] Rerun triggered: {job_name} → new_run_id={new_run_id}")
        return new_run_id, True
    except Exception as e:
        print(f"[ActionExecutor] ERROR: failed to rerun {job_name}: {e}")
        return None, False

# COMMAND ----------

def queue_for_approval(failure, event_id):
    """Writes a PENDING row to AgentApprovalQueue."""
    if dry_run:
        print(f"[ActionExecutor] DRY RUN — skipping queue write for {failure['job_name']} (EventId={event_id})")
        return

    from pyspark.sql.types import StructType, StructField, StringType, TimestampType

    schema = StructType([
        StructField("EventId", StringType(), True),
        StructField("JobId", StringType(), True),
        StructField("RunId", StringType(), True),
        StructField("RequestedAction", StringType(), True),
        StructField("RequestedAt", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("ReviewedBy", StringType(), True),
        StructField("ReviewedAt", TimestampType(), True)
    ])

    row_data = [(
        event_id,
        failure["job_id"],
        failure["run_id"],
        failure.get("recommended_action", "RERUN"),
        datetime.now(timezone.utc),
        "PENDING",
        None,
        None
    )]

    approval_df = spark.createDataFrame(row_data, schema=schema)
    approval_df.write.format("delta").mode("append").saveAsTable(approval_table)

    print(f"[ActionExecutor] Approval queued: {failure['job_name']} (EventId={event_id})")
    print(f"[ActionExecutor] To approve, run in DBSQL:")
    print(f"  UPDATE {approval_table}")
    print(f"  SET Status='APPROVED', ReviewedBy='<your-email>', ReviewedAt=current_timestamp()")
    print(f"  WHERE EventId='{event_id}';")

# COMMAND ----------

execution_results = []

for failure in guardrail_results:
    event_id = str(uuid.uuid4())
    job_name = failure["job_name"]
    status = failure.get("guardrail_status")

    if status in ("BLOCKED_LOW_CONFIDENCE", "BLOCKED_CAP"):
        print(f"[ActionExecutor] No action: {job_name} — {failure.get('block_reason')}")
        execution_results.append({
            **failure,
            "event_id": event_id,
            "new_run_id": None,
            "final_status": status
        })

    elif status == "PENDING_APPROVAL":
        queue_for_approval(failure, event_id)
        execution_results.append({
            **failure,
            "event_id": event_id,
            "new_run_id": None,
            "final_status": "PENDING_APPROVAL"
        })

    elif status == "AUTO_APPROVED":
        new_run_id, success = trigger_rerun(failure["job_id"], job_name)
        final_status = (
            "RERUN_TRIGGERED"
            if success and new_run_id != "DRY_RUN_SKIPPED"
            else ("DRY_RUN" if dry_run else "RERUN_FAILED")
        )
        execution_results.append({
            **failure,
            "event_id": event_id,
            "new_run_id": new_run_id if new_run_id != "DRY_RUN_SKIPPED" else None,
            "final_status": final_status
        })

    else:
        execution_results.append({
            **failure,
            "event_id": event_id,
            "new_run_id": None,
            "final_status": status or "UNKNOWN_STATE"
        })
