# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PipelineMonitor-003-GuardrailEngine
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Applies three safety checks before any automated action is taken.
# MAGIC
# MAGIC | Check | Rule | Outcome |
# MAGIC |:-----:|:----:|:-------:|
# MAGIC | 1 | `confidence < confidence_threshold` | `BLOCKED_LOW_CONFIDENCE` |
# MAGIC | 2 | `recommended_action not in safe_actions` | `PENDING_APPROVAL` |
# MAGIC | 3 | `reruns_today >= max_auto_reruns_per_job_per_day` | `BLOCKED_CAP` |
# MAGIC | — | All checks pass | `AUTO_APPROVED` |
# MAGIC
# MAGIC ### Inputs (set by 000-Master / 002-LLMAnalyzer before `%run`)
# MAGIC
# MAGIC | Variable | Description |
# MAGIC |:--------:|:-----------:|
# MAGIC | `policy` | `PipelineMonitor_policy.json` as a Python dict |
# MAGIC | `audit_catalog` | Unity Catalog catalog name |
# MAGIC | `audit_schema` | Schema name |
# MAGIC | `analyzed_failures` | List from 002-LLMAnalyzer |
# MAGIC
# MAGIC ### Output
# MAGIC - `guardrail_results` — `analyzed_failures` enriched with: `auto_execute`, `requires_approval`, `action_tier`, `block_reason`, `guardrail_status`
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date | Author | Description | Type Of Change |
# MAGIC |:----:|:------:|:-----------:|:--------------:|
# MAGIC | 2026-08-11 | Mosaic Team | Initial implementation | Feature |

# COMMAND ----------

confidence_threshold = float(policy.get("confidence_threshold", 0.70))
max_reruns = int(policy.get("max_auto_reruns_per_job_per_day", 3))
safe_actions = policy.get("safe_actions", ["RERUN"])
audit_table = f"{audit_catalog}.{audit_schema}.AgentActionLog"

# COMMAND ----------

def get_rerun_count_today(job_id):
    """Return number of RERUN_TRIGGERED events for this job today."""
    try:
        row = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {audit_table}
            WHERE JobId = '{job_id}'
            AND Status = 'RERUN_TRIGGERED'
            AND DATE(CreatedAt) = CURRENT_DATE()
        """).collect()[0]
        return int(row["cnt"])
    except Exception:
        return 0

# COMMAND ----------

guardrail_results = []

for failure in analyzed_failures:
    job_id = failure["job_id"]
    job_name = failure["job_name"]
    confidence = float(failure.get("confidence", 0.0))
    recommended_action = failure.get("recommended_action", "ALERT_ONLY")

    # Check 1 — Confidence threshold
    if confidence < confidence_threshold:
        guardrail_results.append({
            **failure,
            "auto_execute": False,
            "requires_approval": False,
            "action_tier": "RESTRICTED",
            "block_reason": f"Confidence {confidence:.2f} is below threshold {confidence_threshold}",
            "guardrail_status": "BLOCKED_LOW_CONFIDENCE"
        })
        print(f"[GuardrailEngine] BLOCKED (low confidence)  | {job_name} | conf={confidence:.2f}")
        continue

    # Check 2 — Action tier
    if recommended_action not in safe_actions:
        guardrail_results.append({
            **failure,
            "auto_execute": False,
            "requires_approval": True,
            "action_tier": "RESTRICTED",
            "block_reason": f"Action '{recommended_action}' is not in safe_actions — human approval required",
            "guardrail_status": "PENDING_APPROVAL"
        })
        print(f"[GuardrailEngine] APPROVAL REQUIRED          | {job_name} | action={recommended_action}")
        continue

    # Check 3 — Daily rerun cap
    rerun_count = get_rerun_count_today(job_id)
    if rerun_count >= max_reruns:
        guardrail_results.append({
            **failure,
            "auto_execute": False,
            "requires_approval": False,
            "action_tier": "BLOCKED",
            "block_reason": f"Daily rerun cap reached ({rerun_count}/{max_reruns})",
            "guardrail_status": "BLOCKED_CAP"
        })
        print(f"[GuardrailEngine] BLOCKED (daily cap)        | {job_name} | reruns={rerun_count}/{max_reruns}")
        continue

    # All checks passed
    guardrail_results.append({
        **failure,
        "auto_execute": True,
        "requires_approval": False,
        "action_tier": "SAFE",
        "block_reason": None,
        "guardrail_status": "AUTO_APPROVED"
    })
    print(f"[GuardrailEngine] AUTO-APPROVED                | {job_name} | conf={confidence:.2f} reruns={rerun_count}/{max_reruns}")
