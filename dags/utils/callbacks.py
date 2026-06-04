"""
callbacks.py — Airflow callback functions for Slack alerting.

Provides on_failure_callback and on_success_callback functions used by
the bulk migration DAG for real-time Slack notifications.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from airflow.models import Variable

logger = logging.getLogger(__name__)

# Slack connection configured in Composer environment
_SLACK_CONN_ID = "slack_migration_alerts"
_SLACK_CHANNEL = "#data-migration"


def _get_slack_webhook_url() -> str:
    """Retrieve Slack webhook URL from Airflow Variables."""
    return Variable.get("slack_webhook_url", default_var="")


def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Slack alert on task failure.

    Sends a structured message with task details, execution date,
    exception info, and a link to the Airflow task log.
    """
    try:
        from airflow.providers.slack.operators.slack_webhook import (
            SlackWebhookOperator,
        )
    except ImportError:
        logger.warning("Slack provider not installed — skipping alert")
        return

    task_instance = context.get("task_instance")
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception info")
    log_url = task_instance.log_url if task_instance else ""

    message = (
        f":red_circle: *Bulk Migration Task Failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Error:* ```{str(exception)[:500]}```\n"
        f"<{log_url}|View Task Log>"
    )

    try:
        alert = SlackWebhookOperator(
            task_id="slack_failure_alert",
            slack_webhook_conn_id=_SLACK_CONN_ID,
            message=message,
            channel=_SLACK_CHANNEL,
            username="Migration Bot",
        )
        alert.execute(context=context)
    except Exception:
        logger.exception("Failed to send Slack failure alert")


def on_success_callback(context: Dict[str, Any]) -> None:
    """
    Slack notification on DAG-level success.

    Sends a summary message confirming the bulk migration pipeline completed.
    """
    try:
        from airflow.providers.slack.operators.slack_webhook import (
            SlackWebhookOperator,
        )
    except ImportError:
        logger.warning("Slack provider not installed — skipping alert")
        return

    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    execution_date = context.get("execution_date", "unknown")
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "unknown"

    watermark = Variable.get("bulk_migration_watermark_ts", default_var="NOT SET")

    message = (
        f":white_check_mark: *Bulk Migration Pipeline Complete*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Frozen Watermark (W):* `{watermark}`\n"
        f"All tables loaded and inline validation passed.\n"
        f"Ready for formal validation pipeline."
    )

    try:
        alert = SlackWebhookOperator(
            task_id="slack_success_alert",
            slack_webhook_conn_id=_SLACK_CONN_ID,
            message=message,
            channel=_SLACK_CHANNEL,
            username="Migration Bot",
        )
        alert.execute(context=context)
    except Exception:
        logger.exception("Failed to send Slack success alert")
