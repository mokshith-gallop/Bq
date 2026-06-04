"""
manifest_loader.py — Loads per-table YAML manifests and source-count JSON.

Discovers all YAML manifests under config/tables/{raw,staging,retail,regional}/,
parses them, and groups them by wave assignment for the bulk migration DAG.
Also loads the pre-captured source_counts.json manifest produced during the
capture_source_counts phase.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

# Relative to DAGs folder — Composer syncs config/ alongside dags/
_DEFAULT_CONFIG_ROOT = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/home/airflow/gcs"),
    "dags",
    "config",
    "tables",
)

# Wave names matching the YAML `wave:` field
WAVE_1_SMALL = "wave_1_small"
WAVE_2_MEDIUM = "wave_2_medium"
WAVE_3_LARGE = "wave_3_large"
WAVE_EU = "eu"

ALL_WAVES = [WAVE_1_SMALL, WAVE_2_MEDIUM, WAVE_3_LARGE, WAVE_EU]
US_WAVES = [WAVE_1_SMALL, WAVE_2_MEDIUM, WAVE_3_LARGE]


def load_manifest(yaml_path: str) -> Dict[str, Any]:
    """Load and return a single YAML manifest as a dict."""
    with open(yaml_path, "r") as f:
        manifest = yaml.safe_load(f)
    # Inject the manifest file path for traceability
    manifest["_manifest_path"] = yaml_path
    return manifest


def discover_manifests(
    config_root: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Discover all YAML manifests under the config root directory.

    Scans config/tables/{raw,staging,retail,regional}/ for *.yaml files.
    Returns a list of parsed manifest dicts.
    """
    root = Path(config_root or _DEFAULT_CONFIG_ROOT)
    manifests: List[Dict[str, Any]] = []

    for subdir in ["raw", "staging", "retail", "regional"]:
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

    logger.info("Discovered %d table manifests from %s", len(manifests), root)
    return manifests


def group_by_wave(
    manifests: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group manifests by their wave assignment.

    Returns dict keyed by wave name (wave_1_small, wave_2_medium,
    wave_3_large, eu) with list of manifests as values.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {w: [] for w in ALL_WAVES}

    for m in manifests:
        wave = m.get("wave", WAVE_1_SMALL)
        if wave not in groups:
            logger.warning(
                "Unknown wave '%s' for %s.%s — defaulting to wave_1_small",
                wave,
                m.get("source", {}).get("database"),
                m.get("source", {}).get("table"),
            )
            wave = WAVE_1_SMALL
        groups[wave].append(m)

    for wave_name, tables in groups.items():
        logger.info("Wave %s: %d tables", wave_name, len(tables))

    return groups


def get_table_id(manifest: Dict[str, Any]) -> str:
    """Return a unique table identifier: database__table."""
    src = manifest.get("source", {})
    return f"{src.get('database', 'unknown')}__{src.get('table', 'unknown')}"


def get_gcs_manifest_path(manifest: Dict[str, Any], gcs_config_root: str) -> str:
    """
    Return the GCS URI of this manifest file for passing to Dataproc jobs.

    The DAG uploads manifests to GCS; this computes the target path.
    e.g. gs://acme-migration-staging-us/config/tables/raw/sales_retail.yaml
    """
    src = manifest.get("source", {})
    db = src.get("database", "unknown")
    table = src.get("table", "unknown")
    # Map database names to config subdirectories
    db_dir_map = {
        "raw": "raw",
        "staging": "staging",
        "retail": "retail",
        "regional": "regional",
    }
    subdir = db_dir_map.get(db, db)
    return f"{gcs_config_root}/config/tables/{subdir}/{table}.yaml"


def load_source_counts(gcs_path: str) -> Dict[str, int]:
    """
    Load source row counts from the JSON manifest file.

    The file is a JSON object mapping "database.table" keys to integer counts.
    Example: {"raw.sales_retail": 2847293, "retail.fact_sales": 58201344, ...}

    In Composer, this file is downloaded from GCS by the capture_source_counts
    task and stored locally, or read directly from GCS via gcsfs.

    Args:
        gcs_path: Local file path or GCS URI of source_counts.json.

    Returns:
        Dict mapping "database.table" → expected row count.
    """
    # If it's a local path (e.g. /tmp/source_counts.json after download)
    if os.path.isfile(gcs_path):
        with open(gcs_path, "r") as f:
            return json.load(f)

    # For GCS paths, use gcsfs (available in Composer)
    try:
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        with fs.open(gcs_path, "r") as f:
            return json.load(f)
    except ImportError:
        logger.warning(
            "gcsfs not available — cannot read GCS path: %s", gcs_path
        )
        return {}


def get_expected_row_count(
    source_counts: Dict[str, int],
    manifest: Dict[str, Any],
) -> Optional[int]:
    """Look up the expected row count for a manifest from source_counts."""
    src = manifest.get("source", {})
    key = f"{src.get('database')}.{src.get('table')}"
    return source_counts.get(key)
