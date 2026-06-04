"""
conftest.py — Shared fixtures for the bulk migration pipeline test suite.

Provides:
  - PySpark session fixture (requires Java; auto-skips if unavailable)
  - Sample manifest builders for every format type
  - Sample DataFrame generators for transform testing
  - Temporary directory fixtures for YAML / JSON test files
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml

# ---------------------------------------------------------------------------
# PySpark session fixture (skips if no Java / PySpark runtime)
# ---------------------------------------------------------------------------

_SPARK_AVAILABLE = None


def _check_spark():
    """Lazy-check whether PySpark can start."""
    global _SPARK_AVAILABLE
    if _SPARK_AVAILABLE is not None:
        return _SPARK_AVAILABLE
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        spark.stop()
        _SPARK_AVAILABLE = True
    except Exception:
        _SPARK_AVAILABLE = False
    return _SPARK_AVAILABLE


@pytest.fixture(scope="session")
def spark():
    """Session-scoped PySpark SparkSession.

    Auto-skips the test if Java / PySpark is not available.
    """
    if not _check_spark():
        pytest.skip("PySpark not available (Java not installed)")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("bulk_load_tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Manifest builder helpers
# ---------------------------------------------------------------------------

def _base_manifest(
    database: str = "raw",
    table: str = "test_table",
    cluster: str = "acme-lake",
    fmt: str = "parquet",
    gcs_path: str = "gs://test-bucket/raw/test_table/",
    partition_cols: Optional[List[str]] = None,
    format_options: Optional[Dict[str, Any]] = None,
    project: str = "acme-analytics",
    dataset: str = "raw",
    target_table: Optional[str] = None,
    partition_key_conversion: Any = None,
    map_to_json: Optional[List[Dict]] = None,
    kudu_epoch_conversion: Optional[List[Dict]] = None,
    acid_compaction: bool = False,
    type_widening: Optional[List[Dict]] = None,
    watermark_col: Optional[str] = None,
    null_check_cols: Optional[List[str]] = None,
    wave: str = "wave_1_small",
    migration_tier: str = "standard",
) -> Dict[str, Any]:
    """Build a minimal valid manifest dict for testing."""
    manifest: Dict[str, Any] = {
        "source": {
            "database": database,
            "table": table,
            "cluster": cluster,
            "format": fmt,
            "gcs_path": gcs_path,
            "partition_cols": partition_cols or [],
        },
        "target": {
            "project": project,
            "dataset": dataset,
            "table": target_table or table,
        },
        "transforms": {
            "partition_key_conversion": partition_key_conversion,
            "map_to_json": map_to_json or [],
            "type_widening": type_widening or [],
        },
        "validation": {
            "watermark_col": watermark_col,
            "null_check_cols": null_check_cols or [],
        },
        "wave": wave,
        "migration_tier": migration_tier,
    }
    if format_options:
        manifest["source"]["format_options"] = format_options
    if kudu_epoch_conversion:
        manifest["transforms"]["kudu_epoch_conversion"] = kudu_epoch_conversion
    if acid_compaction:
        manifest["source"]["acid_compaction"] = True
        manifest["transforms"]["acid_compaction"] = True
    return manifest


@pytest.fixture
def make_manifest():
    """Factory fixture that returns a manifest builder function."""
    return _base_manifest


# ---------------------------------------------------------------------------
# Pre-built manifests for specific format types
# ---------------------------------------------------------------------------

@pytest.fixture
def csv_manifest():
    """Manifest for a textfile_csv table (sales_retail-like)."""
    return _base_manifest(
        table="sales_retail",
        fmt="textfile_csv",
        gcs_path="gs://test-bucket/raw/sales/",
        partition_cols=["date_ts"],
        format_options={"delimiter": ",", "header": True, "null_value": ""},
        partition_key_conversion={
            "source_col": "date_ts",
            "target_col": "partition_date",
            "parse_format": "%Y%m%d",
            "parse_fn": "PARSE_DATE",
            "generated_column": False,
        },
        watermark_col="date_ts",
        null_check_cols=["invoice_no"],
        wave="wave_3_large",
        migration_tier="critical",
    )


@pytest.fixture
def tsv_manifest():
    """Manifest for a textfile_tsv table (omniture_logs-like)."""
    return _base_manifest(
        table="omniture_logs",
        fmt="textfile_tsv",
        gcs_path="gs://test-bucket/raw/weblogs/",
        partition_cols=["date_ts"],
        format_options={"delimiter": "\t", "header": False, "column_count": 60},
        partition_key_conversion={
            "source_col": "date_ts",
            "target_col": "partition_date",
            "parse_format": "%Y%m%d",
            "parse_fn": "PARSE_DATE",
            "generated_column": False,
        },
        wave="wave_3_large",
    )


@pytest.fixture
def regex_manifest():
    """Manifest for a regex_serde table (loyalty_events-like)."""
    return _base_manifest(
        table="loyalty_events",
        fmt="regex_serde",
        gcs_path="gs://test-bucket/raw/loyalty_events/",
        partition_cols=["date_ts"],
        format_options={
            "regex_pattern": r"^([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|TX:([^;]+);META:(.*)$",
            "regex_columns": [
                "event_ts_str", "member_id", "event_type", "points",
                "store_id", "tx_id", "meta_raw",
            ],
        },
        partition_key_conversion={
            "source_col": "date_ts",
            "target_col": "partition_date",
            "parse_format": "%Y%m%d",
            "parse_fn": "PARSE_DATE",
            "generated_column": False,
        },
    )


@pytest.fixture
def generated_col_manifest():
    """Manifest for a table with a BQ generated column (fact_inventory_movements-like)."""
    return _base_manifest(
        database="retail",
        table="fact_inventory_movements",
        cluster="acme-analytics",
        fmt="parquet",
        dataset="retail",
        gcs_path="gs://test-bucket/retail/fact_inventory_movements/",
        partition_cols=["year", "month", "day", "region"],
        partition_key_conversion={
            "source_cols": ["year", "month", "day"],
            "target_col": "partition_date",
            "parse_fn": "DATE",
            "generated_column": True,
        },
        wave="wave_3_large",
    )


@pytest.fixture
def kudu_manifest():
    """Manifest for a Kudu snapshot table (inventory_realtime_snapshot-like)."""
    return _base_manifest(
        database="retail",
        table="kudu_inventory_realtime",
        cluster="acme-analytics",
        fmt="kudu",
        dataset="retail",
        target_table="inventory_realtime_snapshot",
        gcs_path="gs://test-bucket/retail/kudu_inventory_realtime/",
        kudu_epoch_conversion=[
            {"col": "last_updated_ts", "from_unit": "millis"},
        ],
    )


@pytest.fixture
def acid_manifest():
    """Manifest for an ACID ORC table (returns_ledger-like)."""
    return _base_manifest(
        database="retail",
        table="returns_ledger",
        cluster="acme-analytics",
        fmt="orc",
        dataset="retail",
        gcs_path="gs://test-bucket/retail/returns_ledger/",
        acid_compaction=True,
        wave="wave_3_large",
        migration_tier="critical",
        null_check_cols=["return_id", "invoice_no"],
    )


@pytest.fixture
def map_to_json_manifest():
    """Manifest for a table with MAP→JSON conversion (mobile_events-like)."""
    return _base_manifest(
        table="mobile_events",
        fmt="json_serde",
        gcs_path="gs://test-bucket/raw/mobile_events/",
        partition_cols=["event_date", "hour_bucket"],
        format_options={"ndjson": True},
        map_to_json=[{"source_col": "properties", "target_col": "properties"}],
        partition_key_conversion={
            "source_col": "event_date",
            "target_col": "partition_date",
            "parse_format": "%Y%m%d",
            "parse_fn": "PARSE_DATE",
            "generated_column": True,
        },
        wave="wave_3_large",
    )


@pytest.fixture
def multi_col_partition_manifest():
    """Manifest for multi-column INT partition (fact_payments-like)."""
    return _base_manifest(
        database="retail",
        table="fact_payments",
        cluster="acme-analytics",
        fmt="parquet",
        dataset="retail",
        gcs_path="gs://test-bucket/retail/fact_payments/",
        partition_cols=["post_year", "post_month", "payment_method_partition"],
        partition_key_conversion={
            "source_cols": ["post_year", "post_month"],
            "target_col": "partition_month",
            "parse_fn": "DATE",
            "generated_column": True,
        },
        wave="wave_3_large",
    )


# ---------------------------------------------------------------------------
# Temporary directory with sample YAML manifests
# ---------------------------------------------------------------------------

@pytest.fixture
def manifest_dir(tmp_path):
    """Create a temporary directory tree with sample manifests for all 4 databases."""
    for db in ("raw", "staging", "retail", "regional"):
        db_dir = tmp_path / db
        db_dir.mkdir()

    # Raw: 3 sample tables covering different formats
    _write_yaml(tmp_path / "raw" / "sales_retail.yaml", _base_manifest(
        table="sales_retail", fmt="textfile_csv",
        format_options={"delimiter": ",", "header": True, "null_value": ""},
        partition_cols=["date_ts"],
    ))
    _write_yaml(tmp_path / "raw" / "loyalty_events.yaml", _base_manifest(
        table="loyalty_events", fmt="regex_serde",
        format_options={
            "regex_pattern": r"^([^|]+)\|(.*)$",
            "regex_columns": ["col1", "col2"],
        },
        partition_cols=["date_ts"],
    ))
    _write_yaml(tmp_path / "raw" / "pos_transactions.yaml", _base_manifest(
        table="pos_transactions", fmt="parquet",
        partition_cols=["date_ts"],
    ))

    # Staging: 2 sample tables
    _write_yaml(tmp_path / "staging" / "cleansed_orders.yaml", _base_manifest(
        database="staging", table="cleansed_orders", fmt="parquet",
        dataset="staging", partition_cols=["order_date"],
    ))
    _write_yaml(tmp_path / "staging" / "parsed_loyalty_events.yaml", _base_manifest(
        database="staging", table="parsed_loyalty_events", fmt="parquet",
        dataset="staging", partition_cols=["date_ts"],
        map_to_json=[{"source_col": "meta", "target_col": "meta"}],
    ))

    # Retail: 3 sample tables covering ACID, Kudu, normal
    _write_yaml(tmp_path / "retail" / "dim_date.yaml", _base_manifest(
        database="retail", table="dim_date", fmt="parquet",
        dataset="retail", cluster="acme-analytics",
    ))
    _write_yaml(tmp_path / "retail" / "returns_ledger.yaml", _base_manifest(
        database="retail", table="returns_ledger", fmt="orc",
        dataset="retail", cluster="acme-analytics",
        acid_compaction=True, wave="wave_3_large",
    ))
    _write_yaml(tmp_path / "retail" / "inventory_realtime_snapshot.yaml", _base_manifest(
        database="retail", table="kudu_inventory_realtime", fmt="kudu",
        dataset="retail", cluster="acme-analytics",
        target_table="inventory_realtime_snapshot",
        kudu_epoch_conversion=[{"col": "last_updated_ts", "from_unit": "millis"}],
    ))

    # Regional: 2 sample tables
    _write_yaml(tmp_path / "regional" / "events_eu.yaml", _base_manifest(
        database="regional", table="events_eu", fmt="parquet",
        dataset="regional_eu", project="acme-analytics-eu",
        cluster="acme-edge", partition_cols=["event_date"], wave="eu",
    ))
    _write_yaml(tmp_path / "regional" / "fact_orders_eu.yaml", _base_manifest(
        database="regional", table="fact_orders_eu", fmt="parquet",
        dataset="regional_eu", project="acme-analytics-eu",
        cluster="acme-edge", partition_cols=["order_year", "order_month"],
        wave="eu",
    ))

    return tmp_path


@pytest.fixture
def source_counts_file(tmp_path):
    """Create a temporary source_counts.json file."""
    counts = {
        "raw.sales_retail": {"row_count": 2847293, "partition_count": 365},
        "raw.pos_transactions": {"row_count": 15000000, "partition_count": 730},
        "retail.dim_date": {"row_count": 73049, "partition_count": None},
        "retail.returns_ledger": {"row_count": 500000, "partition_count": None},
        "regional.events_eu": {"row_count": 1200000, "partition_count": 180},
    }
    path = tmp_path / "source_counts.json"
    path.write_text(json.dumps(counts))
    return str(path)


@pytest.fixture
def legacy_source_counts_file(tmp_path):
    """Create a legacy flat-format source_counts.json file."""
    counts = {
        "raw.sales_retail": 2847293,
        "retail.dim_date": 73049,
    }
    path = tmp_path / "source_counts_legacy.json"
    path.write_text(json.dumps(counts))
    return str(path)


def _write_yaml(path: Path, data: dict):
    """Write a dict as YAML to a file."""
    path.write_text(yaml.dump(data, default_flow_style=False))
