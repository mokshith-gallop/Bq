"""
test_bulk_load.py — Unit tests for spark/bulk_load.py.

Tests cover:
  - Format reader dispatch and registration (all 10 formats)
  - Transformation rules:
    * Partition key conversion (STRING→DATE, multi-col INT→DATE)
    * MAP→JSON conversion with null handling
    * Kudu epoch ms→TIMESTAMP conversion
    * Generated column exclusion
    * Watermark filtering
  - CLI argument parsing
  - Manifest loading and validation

Tests requiring a live PySpark session (Java) are marked with
``@pytest.mark.spark`` and auto-skip if Java is not available.
"""

from __future__ import annotations

import os
import sys
import textwrap
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

# Add project root to path so we can import spark.bulk_load
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================================
# Module import (non-Spark parts work without Java)
# ============================================================================

from spark.bulk_load import (
    _validate_manifest,
    load_manifest,
    parse_args,
    _FORMAT_READERS,
)


# ============================================================================
# 1. Format Reader Registry Tests
# ============================================================================

class TestFormatReaderRegistry:
    """Verify all 10 format handlers are registered."""

    EXPECTED_FORMATS = {
        "textfile_csv", "textfile_tsv", "parquet", "avro",
        "rcfile", "sequencefile", "json_serde", "regex_serde",
        "orc", "kudu",
    }

    def test_all_formats_registered(self):
        """Every expected format has a registered reader function."""
        registered = set(_FORMAT_READERS.keys())
        assert registered == self.EXPECTED_FORMATS, (
            f"Missing: {self.EXPECTED_FORMATS - registered}, "
            f"Extra: {registered - self.EXPECTED_FORMATS}"
        )

    def test_reader_count(self):
        """Exactly 10 format readers are registered."""
        assert len(_FORMAT_READERS) == 10

    def test_readers_are_callable(self):
        """Every registered reader is a callable."""
        for fmt, fn in _FORMAT_READERS.items():
            assert callable(fn), f"Reader for '{fmt}' is not callable"


# ============================================================================
# 2. Manifest Validation Tests
# ============================================================================

class TestManifestValidation:
    """Test _validate_manifest catches missing required fields."""

    def test_valid_manifest(self, make_manifest):
        """A well-formed manifest passes validation without error."""
        m = make_manifest()
        _validate_manifest(m, "test.yaml")  # Should not raise

    def test_missing_source(self, make_manifest):
        m = make_manifest()
        del m["source"]
        with pytest.raises(ValueError, match="source"):
            _validate_manifest(m, "test.yaml")

    def test_missing_target(self, make_manifest):
        m = make_manifest()
        del m["target"]
        with pytest.raises(ValueError, match="target"):
            _validate_manifest(m, "test.yaml")

    def test_missing_transforms(self, make_manifest):
        m = make_manifest()
        del m["transforms"]
        with pytest.raises(ValueError, match="transforms"):
            _validate_manifest(m, "test.yaml")

    def test_missing_wave(self, make_manifest):
        m = make_manifest()
        del m["wave"]
        with pytest.raises(ValueError, match="wave"):
            _validate_manifest(m, "test.yaml")

    def test_missing_source_format(self, make_manifest):
        m = make_manifest()
        del m["source"]["format"]
        with pytest.raises(ValueError, match="format"):
            _validate_manifest(m, "test.yaml")

    def test_missing_target_project(self, make_manifest):
        m = make_manifest()
        del m["target"]["project"]
        with pytest.raises(ValueError, match="project"):
            _validate_manifest(m, "test.yaml")


class TestManifestLoading:
    """Test load_manifest from YAML file."""

    def test_load_local_manifest(self, tmp_path, make_manifest):
        m = make_manifest(table="test_load")
        path = tmp_path / "test.yaml"
        path.write_text(yaml.dump(m))
        loaded = load_manifest(str(path))
        assert loaded["source"]["table"] == "test_load"
        assert loaded["_manifest_path"] == str(path)

    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            load_manifest("/nonexistent/path.yaml")


# ============================================================================
# 3. CLI Argument Parser Tests
# ============================================================================

class TestParseArgs:
    """Test command-line argument parsing."""

    def test_required_args(self):
        args = parse_args([
            "--manifest-path", "config/tables/raw/sales_retail.yaml",
            "--gcs-staging-bucket", "test-bucket",
        ])
        assert args.manifest_path == "config/tables/raw/sales_retail.yaml"
        assert args.gcs_staging_bucket == "test-bucket"
        assert args.watermark_ts is None
        assert args.dry_run is False

    def test_all_args(self):
        args = parse_args([
            "--manifest-path", "gs://bucket/config/tables/raw/t.yaml",
            "--watermark-ts", "2024-06-01T00:00:00Z",
            "--gcs-staging-bucket", "staging-bucket",
            "--dry-run",
        ])
        assert args.manifest_path == "gs://bucket/config/tables/raw/t.yaml"
        assert args.watermark_ts == "2024-06-01T00:00:00Z"
        assert args.gcs_staging_bucket == "staging-bucket"
        assert args.dry_run is True

    def test_missing_required_args(self):
        with pytest.raises(SystemExit):
            parse_args([])


# ============================================================================
# 4. Transformation Rule Tests (with PySpark)
# ============================================================================

@pytest.mark.spark
class TestPartitionKeyConversion:
    """Test Rule 2: partition key conversion."""

    def test_string_to_date_yyyymmdd(self, spark, make_manifest):
        """STRING yyyyMMdd_HH → DATE via PARSE_DATE."""
        from spark.bulk_load import _apply_partition_key_conversion

        df = spark.createDataFrame(
            [("20240315_14", "data1"), ("20240601_00", "data2")],
            ["date_ts", "value"],
        )
        transforms = {
            "partition_key_conversion": {
                "source_col": "date_ts",
                "target_col": "partition_date",
                "parse_fn": "PARSE_DATE",
                "generated_column": False,
            },
        }
        result = _apply_partition_key_conversion(df, transforms)
        rows = result.collect()

        assert "partition_date" in result.columns
        assert "date_ts" in result.columns  # Original preserved
        assert rows[0]["partition_date"] == date(2024, 3, 15)
        assert rows[1]["partition_date"] == date(2024, 6, 1)

    def test_multi_col_int_year_month_day(self, spark):
        """Multi-column INT (year, month, day) → DATE."""
        from spark.bulk_load import _apply_partition_key_conversion

        df = spark.createDataFrame(
            [(2024, 3, 15, "data1"), (2023, 12, 31, "data2")],
            ["year", "month", "day", "value"],
        )
        transforms = {
            "partition_key_conversion": {
                "source_cols": ["year", "month", "day"],
                "target_col": "partition_date",
                "parse_fn": "DATE",
                "generated_column": False,
            },
        }
        result = _apply_partition_key_conversion(df, transforms)
        rows = result.collect()

        assert "partition_date" in result.columns
        assert rows[0]["partition_date"] == date(2024, 3, 15)
        assert rows[1]["partition_date"] == date(2023, 12, 31)

    def test_multi_col_int_year_month(self, spark):
        """Multi-column INT (year, month) → DATE(year, month, 1)."""
        from spark.bulk_load import _apply_partition_key_conversion

        df = spark.createDataFrame(
            [(2024, 6, "data1"), (2023, 1, "data2")],
            ["post_year", "post_month", "value"],
        )
        transforms = {
            "partition_key_conversion": {
                "source_cols": ["post_year", "post_month"],
                "target_col": "partition_month",
                "parse_fn": "DATE",
                "generated_column": False,
            },
        }
        result = _apply_partition_key_conversion(df, transforms)
        rows = result.collect()

        assert "partition_month" in result.columns
        assert rows[0]["partition_month"] == date(2024, 6, 1)
        assert rows[1]["partition_month"] == date(2023, 1, 1)

    def test_generated_column_skips_derivation(self, spark, generated_col_manifest):
        """When generated_column=true, Spark does NOT derive partition_date."""
        from spark.bulk_load import _apply_partition_key_conversion

        df = spark.createDataFrame(
            [(2024, 3, 15, "US", "data")],
            ["year", "month", "day", "region", "value"],
        )
        transforms = generated_col_manifest["transforms"]
        result = _apply_partition_key_conversion(df, transforms)

        # partition_date should NOT be added
        assert "partition_date" not in result.columns
        # Source columns should still be present
        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns

    def test_no_conversion_passthrough(self, spark):
        """When partition_key_conversion is None, DataFrame is unchanged."""
        from spark.bulk_load import _apply_partition_key_conversion

        df = spark.createDataFrame([("val",)], ["col"])
        result = _apply_partition_key_conversion(df, {"partition_key_conversion": None})
        assert result.columns == ["col"]
        assert result.count() == 1


@pytest.mark.spark
class TestMapToJsonConversion:
    """Test Rule 3: MAP→JSON conversion with null handling."""

    def test_map_to_json_basic(self, spark):
        """MAP<STRING,STRING> → JSON string via to_json."""
        from pyspark.sql.types import MapType, StringType
        from spark.bulk_load import _apply_map_to_json

        df = spark.createDataFrame(
            [({"key1": "val1", "key2": "val2"},)],
            schema="properties map<string,string>",
        )
        transforms = {
            "map_to_json": [{"source_col": "properties", "target_col": "properties"}],
        }
        result = _apply_map_to_json(df, transforms)
        row = result.collect()[0]

        import json
        parsed = json.loads(row["properties"])
        assert parsed["key1"] == "val1"
        assert parsed["key2"] == "val2"

    def test_map_null_becomes_json_null(self, spark):
        """Hive MAP NULL → BigQuery JSON NULL (preserved)."""
        from spark.bulk_load import _apply_map_to_json

        df = spark.createDataFrame(
            [(None,)],
            schema="properties map<string,string>",
        )
        transforms = {
            "map_to_json": [{"source_col": "properties", "target_col": "properties"}],
        }
        result = _apply_map_to_json(df, transforms)
        row = result.collect()[0]
        assert row["properties"] is None

    def test_empty_map_becomes_json_empty_object(self, spark):
        """Empty MAP {} → JSON '{}' (not NULL)."""
        from spark.bulk_load import _apply_map_to_json

        df = spark.createDataFrame(
            [({},)],
            schema="properties map<string,string>",
        )
        transforms = {
            "map_to_json": [{"source_col": "properties", "target_col": "properties"}],
        }
        result = _apply_map_to_json(df, transforms)
        row = result.collect()[0]
        assert row["properties"] == "{}"

    def test_map_with_null_value(self, spark):
        """MAP with null value → JSON '{"key": null}'."""
        from spark.bulk_load import _apply_map_to_json

        df = spark.createDataFrame(
            [({"key": None},)],
            schema="properties map<string,string>",
        )
        transforms = {
            "map_to_json": [{"source_col": "properties", "target_col": "properties"}],
        }
        result = _apply_map_to_json(df, transforms)
        row = result.collect()[0]

        import json
        parsed = json.loads(row["properties"])
        assert "key" in parsed
        assert parsed["key"] is None

    def test_no_map_columns_passthrough(self, spark):
        """When map_to_json is empty, DataFrame is unchanged."""
        from spark.bulk_load import _apply_map_to_json

        df = spark.createDataFrame([("val",)], ["col"])
        result = _apply_map_to_json(df, {"map_to_json": []})
        assert result.columns == ["col"]


@pytest.mark.spark
class TestKuduEpochConversion:
    """Test Rule 7: Kudu BIGINT epoch-ms → TIMESTAMP."""

    def test_millis_to_timestamp(self, spark):
        """BIGINT milliseconds → TIMESTAMP."""
        from spark.bulk_load import _apply_kudu_epoch_conversion

        # 2024-06-01T12:00:00Z = 1717243200000 ms
        epoch_ms = 1717243200000
        df = spark.createDataFrame([(epoch_ms,)], ["last_updated_ts"])
        transforms = {
            "kudu_epoch_conversion": [
                {"col": "last_updated_ts", "from_unit": "millis"},
            ],
        }
        result = _apply_kudu_epoch_conversion(df, transforms)
        row = result.collect()[0]

        ts = row["last_updated_ts"]
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.day == 1
        assert ts.hour == 12

    def test_multiple_epoch_columns(self, spark):
        """Multiple epoch columns are all converted."""
        from spark.bulk_load import _apply_kudu_epoch_conversion

        df = spark.createDataFrame(
            [(1717243200000, 1717329600000)],
            ["started_ts", "last_event_ts"],
        )
        transforms = {
            "kudu_epoch_conversion": [
                {"col": "started_ts", "from_unit": "millis"},
                {"col": "last_event_ts", "from_unit": "millis"},
            ],
        }
        result = _apply_kudu_epoch_conversion(df, transforms)
        row = result.collect()[0]

        from pyspark.sql.types import TimestampType
        assert isinstance(result.schema["started_ts"].dataType, TimestampType)
        assert isinstance(result.schema["last_event_ts"].dataType, TimestampType)

    def test_no_kudu_columns_passthrough(self, spark):
        """When kudu_epoch_conversion is empty, DataFrame is unchanged."""
        from spark.bulk_load import _apply_kudu_epoch_conversion

        df = spark.createDataFrame([("val",)], ["col"])
        result = _apply_kudu_epoch_conversion(df, {})
        assert result.columns == ["col"]


@pytest.mark.spark
class TestDropGeneratedColumns:
    """Test generated column exclusion."""

    def test_generated_column_dropped(self, spark):
        """When generated_column=true, target_col is dropped from output."""
        from spark.bulk_load import _drop_generated_columns

        df = spark.createDataFrame(
            [(2024, 3, 15, date(2024, 3, 15))],
            ["year", "month", "day", "partition_date"],
        )
        transforms = {
            "partition_key_conversion": {
                "source_cols": ["year", "month", "day"],
                "target_col": "partition_date",
                "parse_fn": "DATE",
                "generated_column": True,
            },
        }
        result = _drop_generated_columns(df, transforms)
        assert "partition_date" not in result.columns
        assert set(result.columns) == {"year", "month", "day"}

    def test_non_generated_column_kept(self, spark):
        """When generated_column=false, target_col is NOT dropped."""
        from spark.bulk_load import _drop_generated_columns

        df = spark.createDataFrame(
            [("20240315", date(2024, 3, 15))],
            ["date_ts", "partition_date"],
        )
        transforms = {
            "partition_key_conversion": {
                "source_col": "date_ts",
                "target_col": "partition_date",
                "parse_fn": "PARSE_DATE",
                "generated_column": False,
            },
        }
        result = _drop_generated_columns(df, transforms)
        assert "partition_date" in result.columns


@pytest.mark.spark
class TestWatermarkFilter:
    """Test Rule 0: watermark filtering."""

    def test_string_watermark_filter(self, spark, csv_manifest):
        """STRING partition key filtered by watermark."""
        from spark.bulk_load import _apply_watermark_filter

        df = spark.createDataFrame(
            [("20240301_12", "a"), ("20240601_00", "b"), ("20240801_06", "c")],
            ["date_ts", "value"],
        )
        result = _apply_watermark_filter(df, csv_manifest, "2024-06-01T00:00:00Z")
        rows = result.collect()
        # '20240601_00' <= '20240601_99' → included
        # '20240801_06' > '20240601_99' → excluded
        assert len(rows) == 2
        values = {r["value"] for r in rows}
        assert values == {"a", "b"}

    def test_no_watermark_col_skips_filter(self, spark, make_manifest):
        """No watermark_col in manifest → full table loaded."""
        from spark.bulk_load import _apply_watermark_filter

        m = make_manifest(watermark_col=None)
        df = spark.createDataFrame([("a",), ("b",)], ["val"])
        result = _apply_watermark_filter(df, m, "2024-06-01T00:00:00Z")
        assert result.count() == 2

    def test_no_watermark_ts_skips_filter(self, spark, csv_manifest):
        """No watermark_ts provided → full table loaded."""
        from spark.bulk_load import _apply_watermark_filter

        df = spark.createDataFrame([("20990101_00", "a")], ["date_ts", "val"])
        result = _apply_watermark_filter(df, csv_manifest, None)
        assert result.count() == 1


@pytest.mark.spark
class TestRegexSerdeReader:
    """Test regex_serde format reader with loyalty_events pattern."""

    def test_regex_extracts_all_7_columns(self, spark, regex_manifest):
        """RegexSerDe reader extracts all 7 columns from pipe-delimited lines."""
        from spark.bulk_load import _read_regex_serde

        # Create a temp text file with sample loyalty event lines
        import tempfile
        sample_lines = [
            "2024-03-15T14:30:00|MBR001|EARN|150|STORE01|TX:TXN123;META:channel=pos;tier=gold",
            "2024-03-15T15:00:00|MBR002|REDEEM|50|STORE02|TX:TXN456;META:channel=web",
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("\n".join(sample_lines) + "\n")
            tmp_path = f.name

        try:
            # Override gcs_path to use local temp file
            regex_manifest["source"]["gcs_path"] = tmp_path
            df = _read_regex_serde(spark, regex_manifest)
            rows = df.collect()

            assert len(df.columns) == 7
            assert df.columns == [
                "event_ts_str", "member_id", "event_type", "points",
                "store_id", "tx_id", "meta_raw",
            ]
            assert len(rows) == 2
            assert rows[0]["member_id"] == "MBR001"
            assert rows[0]["event_type"] == "EARN"
            assert rows[0]["points"] == "150"
            assert rows[0]["tx_id"] == "TXN123"
            assert rows[0]["meta_raw"] == "channel=pos;tier=gold"
            assert rows[1]["member_id"] == "MBR002"
        finally:
            os.unlink(tmp_path)


@pytest.mark.spark
class TestApplyTransformsIntegration:
    """Integration tests combining multiple transforms."""

    def test_full_transform_pipeline_with_partition_and_map(self, spark, make_manifest):
        """Apply partition conversion + MAP→JSON in one pass."""
        from spark.bulk_load import apply_transforms

        df = spark.createDataFrame(
            [("20240315_12", {"key1": "val1"}, "data")],
            "date_ts string, properties map<string,string>, value string",
        )
        m = make_manifest(
            partition_cols=["date_ts"],
            partition_key_conversion={
                "source_col": "date_ts",
                "target_col": "partition_date",
                "parse_fn": "PARSE_DATE",
                "generated_column": False,
            },
            map_to_json=[{"source_col": "properties", "target_col": "properties"}],
        )
        result = apply_transforms(df, m, None)

        assert "partition_date" in result.columns
        assert "properties" in result.columns
        row = result.collect()[0]
        assert row["partition_date"] == date(2024, 3, 15)

        import json
        assert json.loads(row["properties"])["key1"] == "val1"
