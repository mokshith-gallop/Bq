"""
inline_validation.py — Reusable inline validation task factories.

Generates Airflow tasks that run immediately after each table load to catch
gross failures fast: empty tables, truncated loads, corrupt parsing.

Three checks per table:

  1. **Row count parity**   — ``COUNT(*)`` in BQ == expected from source manifest
  2. **Partition count**    — ``COUNT(DISTINCT partition_col)`` matches expected
  3. **Null key validation** — key columns have no unexpected NULLs

Row count and partition count checks use a PythonOperator that downloads
source_counts.json from GCS at runtime (the file is produced during Phase 1
of the DAG and is not available at DAG parse time).  Null key checks use a
BigQueryCheckOperator with static SQL (0 NULLs expected in key columns).

These are intentionally lightweight smoke tests.  The full 4-layer validation
(column aggregates, row-level fingerprints, BAQs) runs in a separate
downstream pipeline.

Usage in the DAG::

    from dags.operators.inline_validation import build_inline_validation_tasks

    tasks = build_inline_validation_tasks(
        manifest=manifest,
        source_counts_gcs_path="gs://bucket/manifests/source_counts.json",
        gcp_conn_id="google_cloud_default",
    )
    for vt in tasks:
        load_task >> vt
"""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Any, Dict, List, Optional

from airflow.operators.python import PythonOperator
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
SELECT COUNT(*) AS row_count
FROM `{project}.{dataset}.{table}`
"""

_PARTITION_COUNT_SQL = """\
SELECT COUNT(DISTINCT {partition_col}) AS partition_count
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
# Runtime check callables (used by PythonOperator)
# ============================================================================


def _download_source_counts(gcs_path: str) -> Dict[str, Any]:
    """Download and parse source_counts.json from GCS.

    Uses gsutil (available in Composer) as a simple, dependency-free
    fallback.  Falls back to gcsfs if available.
    """
    try:
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        with fs.open(gcs_path, "r") as fh:
            return json.load(fh)
    except ImportError:
        pass

    # Fallback: gsutil cp to a temp file
    import tempfile
    tmp = tempfile.mktemp(suffix=".json")
    try:
        subprocess.check_call(
            ["gsutil", "cp", gcs_path, tmp],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with open(tmp, "r") as fh:
            return json.load(fh)
    finally:
        import os
        if os.path.exists(tmp):
            os.unlink(tmp)


def _check_row_count(
    project: str,
    dataset: str,
    table: str,
    table_key: str,
    source_counts_gcs_path: str,
    gcp_conn_id: str = "google_cloud_default",
    **context: Any,
) -> None:
    """Runtime callable: compare BQ row count against source manifest.

    Raises AirflowException on mismatch so the task is marked as failed.
    """
    from airflow.exceptions import AirflowException
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    # 1. Load expected count from source_counts.json
    counts = _download_source_counts(source_counts_gcs_path)
    entry = counts.get(table_key, {})
    if isinstance(entry, dict):
        expected = entry.get("row_count")
    elif isinstance(entry, (int, float)):
        expected = int(entry)
    else:
        expected = None

    if expected is None:
        logger.warning(
            "No expected row count found for %s in %s — skipping check",
            table_key,
            source_counts_gcs_path,
        )
        return

    # 2. Query BQ for actual count
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    sql = _ROW_COUNT_SQL.format(
        project=project, dataset=dataset, table=table,
    )
    result = hook.get_first(sql)
    actual = result[0] if result else 0

    # 3. Compare with exact match (zero delta tolerance)
    if actual != expected:
        raise AirflowException(
            f"Row count mismatch for {project}.{dataset}.{table}: "
            f"expected {expected:,}, got {actual:,} "
            f"(delta: {actual - expected:+,})"
        )
    logger.info(
        "Row count PASS for %s.%s.%s: %s rows",
        project, dataset, table, f"{actual:,}",
    )


def _check_partition_count(
    project: str,
    dataset: str,
    table: str,
    table_key: str,
    partition_col: str,
    source_counts_gcs_path: str,
    gcp_conn_id: str = "google_cloud_default",
    **context: Any,
) -> None:
    """Runtime callable: compare distinct partition count against source manifest.

    Raises AirflowException on mismatch.
    """
    from airflow.exceptions import AirflowException
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    # 1. Load expected count
    counts = _download_source_counts(source_counts_gcs_path)
    entry = counts.get(table_key, {})
    expected = entry.get("partition_count") if isinstance(entry, dict) else None

    if expected is None:
        logger.warning(
            "No expected partition count for %s — skipping check",
            table_key,
        )
        return

    # 2. Query BQ
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    sql = _PARTITION_COUNT_SQL.format(
        project=project,
        dataset=dataset,
        table=table,
        partition_col=partition_col,
    )
    result = hook.get_first(sql)
    actual = result[0] if result else 0

    # 3. Compare
    if actual != expected:
        raise AirflowException(
            f"Partition count mismatch for {project}.{dataset}.{table}: "
            f"expected {expected:,} distinct {partition_col}, "
            f"got {actual:,} (delta: {actual - expected:+,})"
        )
    logger.info(
        "Partition count PASS for %s.%s.%s: %s distinct %s values",
        project, dataset, table, f"{actual:,}", partition_col,
    )


# ============================================================================
# Public API
# ============================================================================


def build_inline_validation_tasks(
    manifest: Dict[str, Any],
    source_counts_gcs_path: str,
    task_id_prefix: str = "",
    gcp_conn_id: str = "google_cloud_default",
) -> list:
    """Build validation tasks for a single migrated table.

    Generates 2-3 Airflow tasks:

    1. **Row count check** (PythonOperator) — downloads source_counts.json
       from GCS at runtime, queries ``COUNT(*)`` in BigQuery, compares with
       exact match.  Skips gracefully if source count is unavailable.

    2. **Partition count check** (PythonOperator) — same runtime resolution
       pattern; checks ``COUNT(DISTINCT partition_col)``.  Only generated
       for partitioned tables.

    3. **Null key check(s)** (BigQueryCheckOperator) — one per key column
       listed in ``validation.null_check_cols``.  Uses static SQL with
       ``expected_null_count=0`` (configurable via manifest
       ``validation.expected_null_counts``).

    Each task fires a Slack alert on failure via ``on_failure_callback``
    but does **not** block sibling tables in the same wave.

    Args:
        manifest:
            Parsed YAML manifest dict for the table.
        source_counts_gcs_path:
            GCS URI of source_counts.json (e.g.
            ``gs://acme-migration-staging-us/manifests/source_counts.json``).
        task_id_prefix:
            Optional prefix for task IDs.
        gcp_conn_id:
            Airflow connection ID for BigQuery.

    Returns:
        List of Airflow operator instances ready for DAG wiring.
    """
    target = manifest.get("target", {})
    project = target.get("project", "acme-analytics")
    dataset = target.get("dataset", "unknown")
    table = target.get("table", "unknown")

    validation_conf = manifest.get("validation", {})
    null_check_cols = validation_conf.get("null_check_cols", [])

    table_key = get_table_key(manifest)
    partition_col = get_bq_partition_column(manifest)

    tasks: list = []

    # ------------------------------------------------------------------
    # Check 1: Row count parity (runtime-resolved from GCS)
    # ------------------------------------------------------------------
    row_count_task = PythonOperator(
        task_id=f"{task_id_prefix}chk_rowcount__{dataset}__{table}",
        python_callable=_check_row_count,
        op_kwargs={
            "project": project,
            "dataset": dataset,
            "table": table,
            "table_key": table_key,
            "source_counts_gcs_path": source_counts_gcs_path,
            "gcp_conn_id": gcp_conn_id,
        },
        on_failure_callback=on_failure_callback,
    )
    tasks.append(row_count_task)

    # ------------------------------------------------------------------
    # Check 2: Partition count parity (partitioned tables only)
    # ------------------------------------------------------------------
    if partition_col is not None:
        partition_count_task = PythonOperator(
            task_id=f"{task_id_prefix}chk_partcount__{dataset}__{table}",
            python_callable=_check_partition_count,
            op_kwargs={
                "project": project,
                "dataset": dataset,
                "table": table,
                "table_key": table_key,
                "partition_col": partition_col,
                "source_counts_gcs_path": source_counts_gcs_path,
                "gcp_conn_id": gcp_conn_id,
            },
            on_failure_callback=on_failure_callback,
        )
        tasks.append(partition_count_task)

    # ------------------------------------------------------------------
    # Check 3: Null key validation (static SQL, 0 NULLs expected)
    # ------------------------------------------------------------------
    expected_null_counts = validation_conf.get("expected_null_counts", {})

    for key_col in null_check_cols:
        expected_nulls = expected_null_counts.get(key_col, 0)

        sql = _NULL_KEY_CHECK_SQL.format(
            project=project,
            dataset=dataset,
            table=table,
            key_col=key_col,
            expected_null_count=expected_nulls,
        )
        null_task = BigQueryCheckOperator(
            task_id=(
                f"{task_id_prefix}chk_nullkey__{dataset}__{table}__{key_col}"
            ),
            sql=sql,
            use_legacy_sql=False,
            gcp_conn_id=gcp_conn_id,
            on_failure_callback=on_failure_callback,
        )
        tasks.append(null_task)

    logger.debug(
        "Built %d validation tasks for %s "
        "(1 row-count + %s partition-count + %d null-key)",
        len(tasks),
        table_key,
        "1" if partition_col else "0",
        len(null_check_cols),
    )

    return tasks


def build_wave_validation_summary_sql(
    manifests: List[Dict[str, Any]],
    source_counts: Dict[str, Dict[str, Optional[int]]],
) -> str:
    """Build a single SQL query that validates row counts for all tables in a wave.

    This is an alternative to per-table PythonOperator checks — a single
    aggregated query that returns one row per table with pass/fail status.
    Useful for generating a summary report after all tables in a wave have
    loaded.

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
        proj = tgt.get("project", "acme-analytics")
        ds = tgt.get("dataset")
        tbl = tgt.get("table")
        expected = get_expected_row_count(source_counts, m)
        if expected is None:
            continue

        union_parts.append(
            f"  SELECT '{ds}.{tbl}' AS table_name, "
            f"{expected} AS expected_rows, "
            f"cnt AS actual_rows, "
            f"CASE WHEN cnt = {expected} THEN 'PASS' ELSE 'FAIL' END AS status "
            f"FROM (SELECT COUNT(*) AS cnt FROM `{proj}.{ds}.{tbl}`)"
        )

    if not union_parts:
        return "SELECT 'No tables to validate' AS status"

    return "\nUNION ALL\n".join(union_parts)
