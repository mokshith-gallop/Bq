"""
manifest_loader.py — Loads per-table YAML manifests and source-count JSON.

Discovers all YAML manifests under config/tables/{raw,staging,retail,regional}/,
parses them, and groups them by wave assignment for the bulk migration DAG.
Also loads the pre-captured source_counts.json manifest produced during the
capture_source_counts phase.

Source counts JSON format:
    {
        "raw.sales_retail": {"row_count": 2847293, "partition_count": 365},
        "retail.fact_sales": {"row_count": 58201344, "partition_count": 1095},
        "retail.dim_date": {"row_count": 73049, "partition_count": null},
        ...
    }

    Legacy flat format (row counts only) is also supported:
    {
        "raw.sales_retail": 2847293,
        ...
    }
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

logger = logging.getLogger(__name__)

# Relative to DAGs folder — Composer syncs config/ alongside dags/
_DEFAULT_CONFIG_ROOT = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/home/airflow/gcs"),
    "dags",
    "config",
    "tables",
)

# Database subdirectories
DATABASE_SUBDIRS = ("raw", "staging", "retail", "regional")

# Wave names matching the YAML `wave:` field
WAVE_1_SMALL = "wave_1_small"
WAVE_2_MEDIUM = "wave_2_medium"
WAVE_3_LARGE = "wave_3_large"
WAVE_EU = "eu"

ALL_WAVES = [WAVE_1_SMALL, WAVE_2_MEDIUM, WAVE_3_LARGE, WAVE_EU]
US_WAVES = [WAVE_1_SMALL, WAVE_2_MEDIUM, WAVE_3_LARGE]


# ============================================================================
# Manifest discovery and loading
# ============================================================================


def load_manifest(yaml_path: str) -> Dict[str, Any]:
    """Load and return a single YAML manifest as a dict.

    Args:
        yaml_path: Absolute or relative path to the YAML manifest file.

    Returns:
        Parsed manifest dict with an injected ``_manifest_path`` key.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        yaml.YAMLError: If the file is not valid YAML.
    """
    with open(yaml_path, "r") as fh:
        manifest = yaml.safe_load(fh)
    if not isinstance(manifest, dict):
        raise ValueError(f"Manifest is not a dict: {yaml_path}")
    # Inject the file path for traceability in logs and error messages
    manifest["_manifest_path"] = yaml_path
    return manifest


def discover_manifests(
    config_root: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Discover all YAML manifests under the config root directory.

    Scans ``config/tables/{raw,staging,retail,regional}/`` for ``*.yaml``
    files, parses each one, and returns a list of manifest dicts sorted
    by database then table name for deterministic ordering.

    Args:
        config_root: Root directory containing database subdirectories.
            Defaults to ``$AIRFLOW_HOME/dags/config/tables/``.

    Returns:
        List of parsed manifest dicts.

    Raises:
        RuntimeError: If no manifests are found (likely a config error).
    """
    root = Path(config_root or _DEFAULT_CONFIG_ROOT)
    manifests: List[Dict[str, Any]] = []

    for subdir in DATABASE_SUBDIRS:
        search_dir = root / subdir
        if not search_dir.is_dir():
            logger.warning("Config directory not found: %s", search_dir)
            continue
        for yaml_file in sorted(search_dir.glob("*.yaml")):
            try:
                manifest = load_manifest(str(yaml_file))
                manifests.append(manifest)
            except Exception:
                logger.exception("Failed to load manifest: %s", yaml_file)
                raise

    if not manifests:
        raise RuntimeError(
            f"No manifests discovered under {root}. "
            f"Checked subdirectories: {DATABASE_SUBDIRS}"
        )

    logger.info("Discovered %d table manifests from %s", len(manifests), root)
    return manifests


# ============================================================================
# Wave grouping
# ============================================================================


def group_by_wave(
    manifests: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group manifests by their ``wave`` assignment.

    Returns:
        Dict keyed by wave name (``wave_1_small``, ``wave_2_medium``,
        ``wave_3_large``, ``eu``) with list of manifests as values.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {w: [] for w in ALL_WAVES}

    for m in manifests:
        wave = m.get("wave", WAVE_1_SMALL)
        if wave not in groups:
            logger.warning(
                "Unknown wave '%s' for %s.%s — defaulting to %s",
                wave,
                m.get("source", {}).get("database"),
                m.get("source", {}).get("table"),
                WAVE_1_SMALL,
            )
            wave = WAVE_1_SMALL
        groups[wave].append(m)

    for wave_name, tables in groups.items():
        logger.info("Wave %s: %d tables", wave_name, len(tables))

    return groups


def group_by_cluster(
    manifests: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group manifests by their source cluster.

    Returns:
        Dict keyed by cluster name (``acme-lake``, ``acme-analytics``,
        ``acme-edge``) with list of manifests as values.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for m in manifests:
        cluster = m.get("source", {}).get("cluster", "unknown")
        groups.setdefault(cluster, []).append(m)
    return groups


# ============================================================================
# Table identification
# ============================================================================


def get_table_key(manifest: Dict[str, Any]) -> str:
    """Return ``database.table`` key matching source_counts.json format.

    Example: ``"raw.sales_retail"``
    """
    src = manifest.get("source", {})
    return f"{src.get('database', 'unknown')}.{src.get('table', 'unknown')}"


def get_table_id(manifest: Dict[str, Any]) -> str:
    """Return a unique Airflow-safe task ID fragment: ``database__table``.

    Example: ``"raw__sales_retail"``
    """
    src = manifest.get("source", {})
    return f"{src.get('database', 'unknown')}__{src.get('table', 'unknown')}"


def get_bq_fqn(manifest: Dict[str, Any]) -> str:
    """Return fully-qualified BigQuery table reference.

    Example: ``"acme-analytics.raw.sales_retail"``
    """
    tgt = manifest.get("target", {})
    return f"{tgt.get('project')}.{tgt.get('dataset')}.{tgt.get('table')}"


def get_gcs_manifest_path(manifest: Dict[str, Any], gcs_config_root: str) -> str:
    """Return the GCS URI of this manifest file for Dataproc jobs.

    The DAG uploads manifests to GCS; this computes the target path.

    Example: ``gs://acme-migration-staging-us/config/tables/raw/sales_retail.yaml``
    """
    src = manifest.get("source", {})
    db = src.get("database", "unknown")
    tgt_table = manifest.get("target", {}).get("table", "unknown")
    # Map database to config subdirectory
    db_dir = {
        "raw": "raw",
        "staging": "staging",
        "retail": "retail",
        "regional": "regional",
    }.get(db, db)
    return f"{gcs_config_root}/config/tables/{db_dir}/{tgt_table}.yaml"


# ============================================================================
# Partition column detection
# ============================================================================


def get_bq_partition_column(manifest: Dict[str, Any]) -> Optional[str]:
    """Determine the BigQuery partition column from the manifest.

    Logic:
      1. If ``transforms.partition_key_conversion`` exists, use ``target_col``
         (e.g. ``partition_date``, ``partition_month``).
      2. If no conversion but a single native DATE partition column exists in
         ``source.partition_cols``, use it directly (e.g. ``sale_date``,
         ``snapshot_date``).
      3. If there are no partition cols, or the partition is a non-date
         STRING (e.g. ``snapshot_hour``), return None.

    Returns:
        BigQuery partition column name, or None for non-partitioned tables.
    """
    transforms = manifest.get("transforms", {})
    pkc = transforms.get("partition_key_conversion")

    if pkc is not None:
        # Explicit conversion defined — the target column is the BQ partition
        return pkc.get("target_col")

    # No partition_key_conversion — check for native DATE partition passthrough
    partition_cols = manifest.get("source", {}).get("partition_cols", [])
    if not partition_cols:
        return None

    # For multi-column partitions where the first column is a native DATE
    # (e.g. [event_date, platform_partition] → BQ PARTITION BY event_date),
    # the first column passes through as the BQ partition column.
    # Check the first column for DATE-like naming patterns.
    col = partition_cols[0]

    # Known native DATE partition column patterns from the Hive DDLs:
    #   sale_date, order_date, load_date, snapshot_date, score_date,
    #   return_date, refund_date, event_date, pick_date, start_date,
    #   decision_date, redemption_date, created_date, consent_date,
    #   ship_date, as_of_date, week_start_date, month_start, period_date
    #
    # Exclude non-date STRING partitions like snapshot_hour, eff_from_year
    _DATE_SUFFIXES = ("_date", "_start")
    _DATE_EXACT = {
        "month_start", "period_date", "as_of_date", "week_start_date",
    }
    if col.endswith(tuple(_DATE_SUFFIXES)) or col in _DATE_EXACT:
        return col

    # First partition column doesn't look like a DATE
    # (e.g. eff_from_year INT, snapshot_hour STRING) — no BQ partition
    return None


def is_partitioned(manifest: Dict[str, Any]) -> bool:
    """Return True if the table has a partition column in BigQuery."""
    return get_bq_partition_column(manifest) is not None


# ============================================================================
# Source counts loading
# ============================================================================


def load_source_counts(
    path: str,
) -> Dict[str, Dict[str, Optional[int]]]:
    """Load source counts from the JSON manifest.

    Supports two formats:

    **Structured format** (preferred)::

        {
            "raw.sales_retail": {"row_count": 2847293, "partition_count": 365},
            "retail.dim_date": {"row_count": 73049, "partition_count": null}
        }

    **Legacy flat format** (row counts only)::

        {"raw.sales_retail": 2847293, "retail.dim_date": 73049}

    Args:
        path: Local file path or GCS URI of source_counts.json.

    Returns:
        Dict mapping ``"database.table"`` → ``{"row_count": int, "partition_count": int|None}``.
    """
    raw_data = _read_json(path)

    # Normalize to structured format
    result: Dict[str, Dict[str, Optional[int]]] = {}
    for key, value in raw_data.items():
        if isinstance(value, dict):
            # Structured format
            result[key] = {
                "row_count": value.get("row_count"),
                "partition_count": value.get("partition_count"),
            }
        elif isinstance(value, (int, float)):
            # Legacy flat format — row count only
            result[key] = {
                "row_count": int(value),
                "partition_count": None,
            }
        else:
            logger.warning(
                "Unexpected value type for %s in source_counts: %s",
                key,
                type(value).__name__,
            )
            result[key] = {"row_count": None, "partition_count": None}

    logger.info("Loaded source counts for %d tables", len(result))
    return result


def _read_json(path: str) -> Dict[str, Any]:
    """Read a JSON file from local filesystem or GCS."""
    if os.path.isfile(path):
        with open(path, "r") as fh:
            return json.load(fh)

    # GCS path — use gcsfs (available in Cloud Composer)
    if path.startswith("gs://"):
        try:
            import gcsfs

            fs = gcsfs.GCSFileSystem()
            with fs.open(path, "r") as fh:
                return json.load(fh)
        except ImportError:
            logger.error(
                "gcsfs not available — cannot read GCS path: %s", path
            )
            return {}
        except Exception:
            logger.exception("Failed to read source counts from GCS: %s", path)
            return {}

    logger.warning("Source counts file not found: %s", path)
    return {}


def get_expected_row_count(
    source_counts: Dict[str, Dict[str, Optional[int]]],
    manifest: Dict[str, Any],
) -> Optional[int]:
    """Look up the expected row count for a manifest from source_counts.

    Args:
        source_counts: Dict from :func:`load_source_counts`.
        manifest: Parsed YAML manifest dict.

    Returns:
        Expected row count, or None if not found.
    """
    key = get_table_key(manifest)
    entry = source_counts.get(key, {})
    if isinstance(entry, dict):
        return entry.get("row_count")
    # Legacy flat format fallback
    if isinstance(entry, (int, float)):
        return int(entry)
    return None


def get_expected_partition_count(
    source_counts: Dict[str, Dict[str, Optional[int]]],
    manifest: Dict[str, Any],
) -> Optional[int]:
    """Look up the expected partition count for a manifest from source_counts.

    Returns None if the table is not partitioned or partition count
    was not captured.

    Args:
        source_counts: Dict from :func:`load_source_counts`.
        manifest: Parsed YAML manifest dict.

    Returns:
        Expected partition count, or None.
    """
    if not is_partitioned(manifest):
        return None

    key = get_table_key(manifest)
    entry = source_counts.get(key, {})
    if isinstance(entry, dict):
        return entry.get("partition_count")
    return None


# ============================================================================
# Convenience: load everything in one call
# ============================================================================


def load_pipeline_config(
    config_root: Optional[str] = None,
    source_counts_path: Optional[str] = None,
) -> Tuple[
    Dict[str, List[Dict[str, Any]]],
    Dict[str, Dict[str, Optional[int]]],
    List[Dict[str, Any]],
]:
    """Load all manifests and source counts in one call.

    Convenience function for DAG initialization.

    Args:
        config_root: Root directory for YAML manifests.
        source_counts_path: Path to source_counts.json.

    Returns:
        Tuple of:
        - ``wave_groups``: manifests grouped by wave
        - ``source_counts``: table → row/partition counts
        - ``all_manifests``: flat list of all manifests
    """
    all_manifests = discover_manifests(config_root)
    wave_groups = group_by_wave(all_manifests)

    source_counts: Dict[str, Dict[str, Optional[int]]] = {}
    if source_counts_path:
        source_counts = load_source_counts(source_counts_path)

    return wave_groups, source_counts, all_manifests
