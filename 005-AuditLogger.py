# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-005-AuditLogger
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Writes every agent decision to `AgentActionLog` using Delta merge (upsert on `EventId`).
# MAGIC Re-running the agent on the same failures will not create duplicate rows.
# MAGIC
# MAGIC ### Inputs (set by 000-Master / 004-ActionExecutor before `%run`)
# MAGIC
# MAGIC | Variable | Description |
# MAGIC |:--------:|:-----------:|
# MAGIC | `audit_catalog` | Unity Catalog catalog name |
# MAGIC | `audit_schema` | Schema name |
# MAGIC | `execution_results` | List from 004-ActionExecutor |
# MAGIC
# MAGIC ### Output
# MAGIC - Rows written/merged to `{audit_catalog}.{audit_schema}.AgentActionLog` with CountryCode column for easy filtering
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-11 | Mosaic Team | Initial implementation | Feature |

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType
)
from datetime import datetime, timezone

try:
    from delta.tables import DeltaTable
    _delta_available = True
except ImportError:
    _delta_available = False

# COMMAND ----------

_LOG_SCHEMA = StructType([
    StructField("EventId",          StringType(),    True),
    StructField("JobId",            StringType(),    True),
    StructField("JobName",          StringType(),    True),
    StructField("CountryCode",      StringType(),    True),
    StructField("RunId",            StringType(),    True),
    StructField("TaskKey",          StringType(),    True),
    StructField("FailureType",      StringType(),    True),
    StructField("TerminationCode",  StringType(),    True),
    StructField("ErrorMessage",     StringType(),    True),
    StructField("Confidence",       DoubleType(),    True),
    StructField("RecommendedAction",StringType(),    True),
    StructField("ActionTier",       StringType(),    True),
    StructField("AutoExecuted",     BooleanType(),   True),
    StructField("Status",           StringType(),    True),
    StructField("NewRunId",         StringType(),    True),
    StructField("Rationale",        StringType(),    True),
    StructField("CreatedAt",        TimestampType(), True)
])

audit_table = f"{audit_catalog}.{audit_schema}.AgentActionLog"
now_utc = datetime.now(timezone.utc)

# COMMAND ----------

if not execution_results:
    print("[AuditLogger] No events to log this cycle.")
else:
    log_rows = [
        (
            r.get("event_id"),
            r.get("job_id"),
            r.get("job_name"),
            r.get("country_code"),
            r.get("run_id"),
            r.get("task_key"),
            r.get("failure_type"),
            r.get("termination_code"),
            r.get("error_message"),
            float(r.get("confidence", 0.0)),
            r.get("recommended_action"),
            r.get("action_tier"),
            bool(r.get("auto_execute", False)),
            r.get("final_status"),
            r.get("new_run_id"),
            r.get("rationale"),
            now_utc
        )
        for r in execution_results
    ]

    log_df = spark.createDataFrame(log_rows, schema=_LOG_SCHEMA)

    table_exists = False
    try:
        spark.sql(f"DESCRIBE TABLE {audit_table}")
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists and _delta_available and DeltaTable.isDeltaTable(spark, audit_table):
        dt = DeltaTable.forName(spark, audit_table)
        (
            dt.alias("tgt")
            .merge(log_df.alias("src"), "tgt.EventId = src.EventId")
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[AuditLogger] Merged {len(log_rows)} row(s) into {audit_table}")
    else:
        log_df.write.format("delta").mode("append").saveAsTable(audit_table)
        print(f"[AuditLogger] Appended {len(log_rows)} row(s) to {audit_table}")

    for r in execution_results:
        print(f"  [{r.get('final_status'):<28}] [{r.get('country_code')}] {r.get('job_name')}  EventId={r.get('event_id')}")
