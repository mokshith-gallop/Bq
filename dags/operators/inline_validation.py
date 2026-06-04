"""
inline_validation.py — Reusable inline validation task factories.

Generates BigQueryCheckOperator tasks that run immediately after each table
load to catch gross failures fast: empty tables, truncated loads, corrupt
parsing. Three checks per table:

  1. Row count parity   — COUNT(*) in BQ == expected from source manifest
  2. Partition count     — COUNT(DISTINCT partition_col) matches expected
  3. Null key validation — key columns have no unexpected NULLs

These are intentionally lightweight smoke tests. The full 4-layer validation
(column aggregates, row-level fingerprints, BAQs) runs in a separate
downstream pipeline.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)

from dags.utils.callbacks import on_failure_callback

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# SQL Templates
# --------------------------------------------------------------------------

_ROW_COUNT_SQL = """
SELECT
  CASE
    WHEN COUNT(*) = {expected_row_count} THEN TRUE
    ELSE ERROR(FORMAT(
      'Row count mismatch for {project}.{dataset}.{table}: expected %d, got %d',
      {expected_row_count}, COUNT(*)))
  END
FROM `{project}.{dataset}.{table}`
"""

_PARTITION_COUNT_SQL = """
SELECT
  CASE
    WHEN COUNT(DISTINCT {partition_col}) = {expected_partition_count} THEN TRUE
    ELSE ERROR(FORMAT(
      'Partition count mismatch for {project}.{dataset}.{table}: expected %d, got %d',
      {expected_partition_count}, COUNT(DISTINCT {partition_col})))
  END
FROM `{project}.{dataset}.{table}`
"""

_NULL_KEY_CHECK_SQL = """
SELECT
  CASE
    WHEN COUNTIF({key_col} IS NULL) = {expected_null_count} THEN TRUE
    ELSE ERROR(FORMAT(
      'Unexpected NULLs in {project}.{dataset}.{table}.{key_col}: expected %d, got %d',
      {expected_null_count}, COUNTIF({key_col} IS NULL)))
  END
FROM `{project}.{dataset}.{table}`
"""


def _get_partition_col(manifest: Dict[str, Any]) -> Optional[str]:
    """
    Determine the BigQuery partition column name from the manifest.

    Checks the partition_key_conversion transform for the target column.
    Returns None if the table is not partitioned in BigQuery.
    """
    transforms = manifest.get("transforms", {})
    pkc = transforms.get("partition_key_conversion")
    if pkc is None:
        # Check if source has native DATE partition columns that pass through
        partition_cols = manifest.get("source", {}).get("partition_cols", [])
        if len(partition_cols) == 1:
            # Single native DATE partition — use it directly
            col = partition_cols[0]
            # Only use if it looks like a date partition (not a STRING key)
            if col.endswith("_date") or col.startswith("date") or col in (
                "sale_date", "order_date", "load_date", "snapshot_date",
                "score_date", "return_date", "refund_date", "event_date",
                "pick_date", "start_date", "decision_date", "redemption_date",
                "created_date", "consent_date", "ship_date", "return_date",
                "as_of_date", "week_start_date", "month_start", "period_date",
                "redemption_date",
            ):
                return col
        return None

    # partition_key_conversion is defined — use the target column
    return pkc.get("target_col")


def build_inline_validation_tasks(
    manifest: Dict[str, Any],
    expected_row_count: Optional[int],
    expected_partition_count: Optional[int] = None,
    task_id_prefix: str = "",
    gcp_conn_id: str = "google_cloud_default",
) -> List[BigQueryCheckOperator]:
    """
    Build 2-3 BigQueryCheckOperator tasks for inline validation of a table.

    Args:
        manifest: Parsed YAML manifest dict for the table.
        expected_row_count: Row count captured from source Hive table.
            If None, row count check is skipped.
        expected_partition_count: Expected number of distinct partition values.
            If None, partition count check is skipped.
        task_id_prefix: Prefix for task IDs (e.g. "validate_").
        gcp_conn_id: Airflow connection ID for BigQuery.

    Returns:
        List of BigQueryCheckOperator tasks to wire into the DAG.
    """
    target = manifest.get("target", {})
    project = target.get("project", "acme-analytics")
    dataset = target.get("dataset", "unknown")
    table = target.get("table", "unknown")
    table_fqn = f"{dataset}.{table}"

    validation_conf = manifest.get("validation", {})
    null_check_cols = validation_conf.get("null_check_cols", [])

    tasks: List[BigQueryCheckOperator] = []

    # ------------------------------------------------------------------
    # Check 1: Row count parity
    # ------------------------------------------------------------------
    if expected_row_count is not None:
        row_count_task = BigQueryCheckOperator(
            task_id=f"{task_id_prefix}validate_rowcount__{dataset}__{table}",
            sql=_ROW_COUNT_SQL.format(
                project=project,
                dataset=dataset,
                table=table,
                expected_row_count=expected_row_count,
            ),
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(row_count_task)

    # ------------------------------------------------------------------
    # Check 2: Partition count parity (partitioned tables only)
    # ------------------------------------------------------------------
    partition_col = _get_partition_col(manifest)
    if partition_col and expected_partition_count is not None:
        partition_count_task = BigQueryCheckOperator(
            task_id=f"{task_id_prefix}validate_partitions__{dataset}__{table}",
            sql=_PARTITION_COUNT_SQL.format(
                project=project,
                dataset=dataset,
                table=table,
                partition_col=partition_col,
                expected_partition_count=expected_partition_count,
            ),
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(partition_count_task)

    # ------------------------------------------------------------------
    # Check 3: Null key validation
    # ------------------------------------------------------------------
    for key_col in null_check_cols:
        null_check_task = BigQueryCheckOperator(
            task_id=(
                f"{task_id_prefix}validate_nullkey__{dataset}__{table}__{key_col}"
            ),
            sql=_NULL_KEY_CHECK_SQL.format(
                project=project,
                dataset=dataset,
                table=table,
                key_col=key_col,
                expected_null_count=0,  # Default: no NULLs expected in key columns
            ),
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(null_check_task)

    return tasks
