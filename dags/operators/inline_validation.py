"""
inline_validation.py — Reusable inline validation task factories.

Generates BigQueryCheckOperator tasks that run immediately after each table
load to catch gross failures fast: empty tables, truncated loads, corrupt
parsing.  Three checks per table:

  1. **Row count parity**   — ``COUNT(*)`` in BQ == expected from source manifest
  2. **Partition count**    — ``COUNT(DISTINCT partition_col)`` matches expected
  3. **Null key validation** — key columns have no unexpected NULLs

These are intentionally lightweight smoke tests.  The full 4-layer validation
(column aggregates, row-level fingerprints, BAQs) runs in a separate
downstream pipeline.

Usage in the DAG::

    from dags.operators.inline_validation import build_inline_validation_tasks
    from dags.utils.manifest_loader import (
        get_expected_row_count, get_expected_partition_count,
    )

    tasks = build_inline_validation_tasks(
        manifest=manifest,
        expected_row_count=get_expected_row_count(source_counts, manifest),
        expected_partition_count=get_expected_partition_count(source_counts, manifest),
    )
    for vt in tasks:
        load_task >> vt
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)

from dags.utils.callbacks import on_failure_callback
from dags.utils.manifest_loader import get_bq_partition_column, get_table_key

logger = logging.getLogger(__name__)


# ============================================================================
# SQL Templates
# ============================================================================

_ROW_COUNT_SQL = """\
SELECT
  CASE
    WHEN COUNT(*) = {expected_row_count} THEN TRUE
    ELSE ERROR(FORMAT(
      'Row count mismatch for {project}.{dataset}.{table}: expected %d, got %d',
      {expected_row_count}, COUNT(*)))
  END
FROM `{project}.{dataset}.{table}`
"""

_PARTITION_COUNT_SQL = """\
SELECT
  CASE
    WHEN COUNT(DISTINCT {partition_col}) = {expected_partition_count} THEN TRUE
    ELSE ERROR(FORMAT(
      'Partition count mismatch for {project}.{dataset}.{table}: '
      'expected %d distinct {partition_col}, got %d',
      {expected_partition_count}, COUNT(DISTINCT {partition_col})))
  END
FROM `{project}.{dataset}.{table}`
"""

_NULL_KEY_CHECK_SQL = """\
SELECT
  CASE
    WHEN COUNTIF({key_col} IS NULL) = {expected_null_count} THEN TRUE
    ELSE ERROR(FORMAT(
      'Unexpected NULLs in {project}.{dataset}.{table}.{key_col}: '
      'expected %d, got %d',
      {expected_null_count}, COUNTIF({key_col} IS NULL)))
  END
FROM `{project}.{dataset}.{table}`
"""


# ============================================================================
# Public API
# ============================================================================


def build_inline_validation_tasks(
    manifest: Dict[str, Any],
    expected_row_count: Optional[int],
    expected_partition_count: Optional[int] = None,
    task_id_prefix: str = "",
    gcp_conn_id: str = "google_cloud_default",
) -> List[BigQueryCheckOperator]:
    """Build 2-3 BigQueryCheckOperator tasks for inline validation.

    Generates validation tasks for a single migrated table.  Each task is
    independent and will fire a Slack alert on failure but will **not**
    block sibling tables in the same wave (Airflow ``trigger_rule`` on
    downstream tasks handles this).

    Args:
        manifest:
            Parsed YAML manifest dict for the table.
        expected_row_count:
            Row count captured from the source Hive table during the
            ``capture_source_counts`` phase.  If ``None``, the row count
            check is skipped (e.g. when source counts haven't been
            captured yet during DAG definition time).
        expected_partition_count:
            Expected number of distinct partition values.
            If ``None``, the partition count check is skipped.
        task_id_prefix:
            Optional prefix for task IDs (e.g. ``"w1_"``).
        gcp_conn_id:
            Airflow connection ID for BigQuery.

    Returns:
        List of ``BigQueryCheckOperator`` tasks ready for DAG wiring.
        May be empty if no checks can be constructed (e.g. no expected
        counts and no null_check_cols).
    """
    target = manifest.get("target", {})
    project = target.get("project", "acme-analytics")
    dataset = target.get("dataset", "unknown")
    table = target.get("table", "unknown")

    validation_conf = manifest.get("validation", {})
    null_check_cols = validation_conf.get("null_check_cols", [])

    tasks: List[BigQueryCheckOperator] = []

    table_key = get_table_key(manifest)

    # ------------------------------------------------------------------
    # Check 1: Row count parity
    # ------------------------------------------------------------------
    if expected_row_count is not None:
        sql = _ROW_COUNT_SQL.format(
            project=project,
            dataset=dataset,
            table=table,
            expected_row_count=expected_row_count,
        )
        task = BigQueryCheckOperator(
            task_id=f"{task_id_prefix}chk_rowcount__{dataset}__{table}",
            sql=sql,
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(task)
        logger.debug(
            "Built row-count check for %s (expected=%d)",
            table_key,
            expected_row_count,
        )
    else:
        logger.info(
            "Skipping row-count check for %s — no expected count available",
            table_key,
        )

    # ------------------------------------------------------------------
    # Check 2: Partition count parity (partitioned tables only)
    # ------------------------------------------------------------------
    partition_col = get_bq_partition_column(manifest)

    if partition_col is not None and expected_partition_count is not None:
        sql = _PARTITION_COUNT_SQL.format(
            project=project,
            dataset=dataset,
            table=table,
            partition_col=partition_col,
            expected_partition_count=expected_partition_count,
        )
        task = BigQueryCheckOperator(
            task_id=f"{task_id_prefix}chk_partcount__{dataset}__{table}",
            sql=sql,
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(task)
        logger.debug(
            "Built partition-count check for %s on column '%s' (expected=%d)",
            table_key,
            partition_col,
            expected_partition_count,
        )
    elif partition_col is not None and expected_partition_count is None:
        logger.info(
            "Skipping partition-count check for %s — no expected count",
            table_key,
        )

    # ------------------------------------------------------------------
    # Check 3: Null key validation
    # ------------------------------------------------------------------
    for key_col in null_check_cols:
        # Default: 0 NULLs expected in key columns.  If a table needs
        # a non-zero expectation (rare), the manifest can override via
        # validation.expected_null_counts: {col: N}.
        expected_null_counts = validation_conf.get("expected_null_counts", {})
        expected_nulls = expected_null_counts.get(key_col, 0)

        sql = _NULL_KEY_CHECK_SQL.format(
            project=project,
            dataset=dataset,
            table=table,
            key_col=key_col,
            expected_null_count=expected_nulls,
        )
        task = BigQueryCheckOperator(
            task_id=(
                f"{task_id_prefix}chk_nullkey__{dataset}__{table}__{key_col}"
            ),
            sql=sql,
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(task)

    if null_check_cols:
        logger.debug(
            "Built %d null-key checks for %s on columns: %s",
            len(null_check_cols),
            table_key,
            null_check_cols,
        )

    return tasks


def build_wave_validation_summary_sql(
    manifests: List[Dict[str, Any]],
    source_counts: Dict[str, Dict[str, Optional[int]]],
) -> str:
    """Build a single SQL query that validates row counts for all tables in a wave.

    This is an alternative to per-table checks — a single aggregated query
    that returns one row per table with pass/fail status.  Useful for
    generating a summary report rather than per-task failures.

    Args:
        manifests: List of manifests in the wave.
        source_counts: Dict from ``load_source_counts()``.

    Returns:
        SQL query string.
    """
    from dags.utils.manifest_loader import get_expected_row_count

    union_parts = []
    for m in manifests:
        tgt = m.get("target", {})
        project = tgt.get("project", "acme-analytics")
        dataset = tgt.get("dataset")
        table = tgt.get("table")
        expected = get_expected_row_count(source_counts, m)
        if expected is None:
            continue

        union_parts.append(
            f"  SELECT '{dataset}.{table}' AS table_name, "
            f"{expected} AS expected_rows, "
            f"cnt AS actual_rows, "
            f"CASE WHEN cnt = {expected} THEN 'PASS' ELSE 'FAIL' END AS status "
            f"FROM (SELECT COUNT(*) AS cnt FROM `{project}.{dataset}.{table}`)"
        )

    if not union_parts:
        return "SELECT 'No tables to validate' AS status"

    return "\nUNION ALL\n".join(union_parts)
