#!/usr/bin/env python3
"""
bulk_load.py — Config-driven PySpark bulk migration job.

Reads a per-table YAML manifest, extracts data from GCS (DistCp'd HDFS copy),
applies in-flight transformations, and writes to BigQuery via the
spark-bigquery-connector (Storage Write API, writeMethod=direct).

Supports 10 source formats:
  textfile_csv, textfile_tsv, parquet, avro, rcfile, sequencefile,
  json_serde, regex_serde, orc (ACID tables), kudu (Kudu snapshots)

Transformation rules applied in-memory:
  - Partition key conversion (STRING→DATE, multi-col INT→DATE)
  - MAP<STRING,STRING> → JSON (via to_json with correct null handling)
  - Kudu epoch ms → TIMESTAMP
  - Type widening (INT/TINYINT/SMALLINT→INT64 — native via connector)
  - ARRAY<STRUCT>/STRUCT pass-through (native via connector)
  - Generated column exclusion (BQ computes them; Spark skips them)
  - Watermark filtering (bound data by frozen watermark W)

Usage (Dataproc Serverless):
    gcloud dataproc batches submit pyspark spark/bulk_load.py -- \\
        --manifest-path gs://acme-migration-staging-us/config/tables/raw/sales_retail.yaml \\
        --watermark-ts '2024-06-01T00:00:00Z' \\
        --gcs-staging-bucket acme-migration-staging-us

Usage (local / Dataproc cluster):
    spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \\
        spark/bulk_load.py \\
        --manifest-path config/tables/raw/sales_retail.yaml \\
        --watermark-ts '2024-06-01T00:00:00Z' \\
        --gcs-staging-bucket acme-migration-staging-us
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("bulk_load")


# ═══════════════════════════════════════════════════════════════════════════
# 1. Manifest Loading
# ═══════════════════════════════════════════════════════════════════════════

def load_manifest(path: str, spark: Optional[SparkSession] = None) -> Dict[str, Any]:
    """Load a YAML manifest from local FS or GCS.

    If the path starts with ``gs://``, the file is read via the Hadoop
    filesystem API through Spark's JVM gateway so that GCS credentials
    configured for Dataproc are honoured transparently.
    """
    if path.startswith("gs://"):
        if spark is None:
            raise ValueError("SparkSession required to read GCS manifest paths")
        jvm = spark.sparkContext._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        gcs_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = gcs_path.getFileSystem(hadoop_conf)
        stream = fs.open(gcs_path)
        reader = jvm.java.io.BufferedReader(
            jvm.java.io.InputStreamReader(stream)
        )
        lines: list[str] = []
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(line)
        reader.close()
        content = "\n".join(lines)
    else:
        content = Path(path).read_text()

    manifest: Dict[str, Any] = yaml.safe_load(content)
    _validate_manifest(manifest, path)
    manifest["_manifest_path"] = path
    return manifest


def _validate_manifest(manifest: Dict[str, Any], path: str = "<unknown>") -> None:
    """Validate that required manifest fields are present."""
    for key in ("source", "target", "transforms", "wave", "validation"):
        if key not in manifest:
            raise ValueError(
                f"Manifest {path} missing required top-level key: '{key}'"
            )
    for key in ("database", "table", "format", "gcs_path"):
        if key not in manifest["source"]:
            raise ValueError(
                f"Manifest {path} source section missing required key: '{key}'"
            )
    for key in ("project", "dataset", "table"):
        if key not in manifest["target"]:
            raise ValueError(
                f"Manifest {path} target section missing required key: '{key}'"
            )


# ═══════════════════════════════════════════════════════════════════════════
# 2. SparkSession Builder
# ═══════════════════════════════════════════════════════════════════════════

def build_spark_session(
    manifest: Dict[str, Any],
    gcs_staging_bucket: str,
) -> SparkSession:
    """Create a SparkSession configured for BigQuery writes and adaptive execution."""
    src = manifest["source"]
    table_name = f"{src['database']}.{src['table']}"
    wave = manifest.get("wave", "unknown")

    builder = (
        SparkSession.builder
        .appName(f"bulk_load__{table_name}__{wave}")
        # Adaptive Query Execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # BigQuery connector settings
        .config("spark.datasource.bigquery.writeMethod", "direct")
        .config("spark.datasource.bigquery.temporaryGcsBucket", gcs_staging_bucket)
        .config("spark.datasource.bigquery.writeAtLeastOneRecord", "false")
    )

    # Wave-specific tuning
    if wave == "wave_3_large":
        builder = builder.config("spark.sql.shuffle.partitions", "200")

    # RCFile and SequenceFile need Hive support
    if src["format"] in ("rcfile", "sequencefile"):
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()


# ═══════════════════════════════════════════════════════════════════════════
# 3. Format-Specific Readers
# ═══════════════════════════════════════════════════════════════════════════

# Registry mapping format name → reader function(spark, manifest) → DataFrame
_FORMAT_READERS: Dict[str, Callable[[SparkSession, Dict[str, Any]], DataFrame]] = {}


def _register_reader(format_name: str):
    """Decorator to register a reader function for a given format."""
    def decorator(fn: Callable[[SparkSession, Dict[str, Any]], DataFrame]):
        _FORMAT_READERS[format_name] = fn
        return fn
    return decorator


def read_source(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Dispatch to the format-specific reader for the table."""
    fmt = manifest["source"]["format"]
    reader_fn = _FORMAT_READERS.get(fmt)
    if reader_fn is None:
        raise ValueError(
            f"No reader registered for format '{fmt}'. "
            f"Available: {sorted(_FORMAT_READERS.keys())}"
        )
    logger.info("Reading source data with format handler: %s", fmt)
    return reader_fn(spark, manifest)


def _gcs_path(manifest: Dict[str, Any]) -> str:
    """Return the GCS path for the source data."""
    return manifest["source"]["gcs_path"]


# ---------------------------------------------------------------------------
# 3a. TEXTFILE CSV (comma-delimited with header)
# ---------------------------------------------------------------------------
@_register_reader("textfile_csv")
def _read_textfile_csv(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read delimited TEXTFILE tables (CSV or TSV with header).

    Handles Hive ``skip.header.line.count=1`` via ``header=True`` and
    ``serialization.null.format=''`` via ``nullValue=''``.  The delimiter
    is taken from manifest ``format_options.delimiter`` (default comma).
    """
    path = _gcs_path(manifest)
    opts = manifest["source"].get("format_options", {})
    delimiter = opts.get("delimiter", ",")
    header = str(opts.get("header", True)).lower()
    null_value = opts.get("null_value", opts.get("null_format", ""))

    logger.info(
        "textfile_csv reader: path=%s, delimiter=%r, header=%s, null_value=%r",
        path, delimiter, header, null_value,
    )
    return (
        spark.read
        .option("header", header)
        .option("inferSchema", "false")
        .option("nullValue", null_value)
        .option("emptyValue", null_value)
        .csv(path, sep=delimiter)
    )


# ---------------------------------------------------------------------------
# 3b. TEXTFILE TSV (tab-delimited, no header — e.g. omniture_logs)
# ---------------------------------------------------------------------------
@_register_reader("textfile_tsv")
def _read_textfile_tsv(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read tab-separated TEXTFILE tables (typically no header).

    Used for omniture_logs: 60 STRING columns, no header, tab-separated.
    When ``header=false`` and ``column_count`` is specified, auto-generated
    column names ``_c0 .. _cN`` are renamed to ``col_1 .. col_N`` to match
    the BigQuery target schema.
    """
    path = _gcs_path(manifest)
    opts = manifest["source"].get("format_options", {})
    has_header = opts.get("header", False)
    column_count = opts.get("column_count", 0)

    logger.info(
        "textfile_tsv reader: path=%s, header=%s, column_count=%d",
        path, has_header, column_count,
    )
    df = (
        spark.read
        .option("header", str(has_header).lower())
        .option("inferSchema", "false")
        .option("sep", "\t")
        .csv(path)
    )

    # When no header, Spark generates _c0, _c1, ... — rename to col_1, col_2, ...
    # to match the BigQuery target schema (e.g. omniture_logs has col_1..col_60)
    if not has_header and column_count > 0:
        for i in range(min(column_count, len(df.columns))):
            df = df.withColumnRenamed(f"_c{i}", f"col_{i + 1}")
        logger.info(
            "Renamed %d columns from _c0..._c%d to col_1...col_%d",
            column_count, column_count - 1, column_count,
        )

    return df


# ---------------------------------------------------------------------------
# 3c. Parquet (native columnar — majority of tables)
# ---------------------------------------------------------------------------
@_register_reader("parquet")
def _read_parquet(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Parquet files — native, no conversion needed."""
    path = _gcs_path(manifest)
    logger.info("parquet reader: path=%s", path)
    return spark.read.parquet(path)


# ---------------------------------------------------------------------------
# 3d. Avro (customer_signups, fraud_signals)
# ---------------------------------------------------------------------------
@_register_reader("avro")
def _read_avro(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Avro files.  Requires spark-avro package on Dataproc."""
    path = _gcs_path(manifest)
    logger.info("avro reader: path=%s", path)
    return spark.read.format("avro").load(path)


# ---------------------------------------------------------------------------
# 3e. RCFile (product_catalog_feed — via Hive compatibility layer)
# ---------------------------------------------------------------------------
@_register_reader("rcfile")
def _read_rcfile(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read RCFile via Hive's SerDe compatibility layer.

    Spark must be launched with Hive support enabled and the Hive SerDe JARs
    available on the classpath.  We read from the Hive metastore table that
    has been registered on the Dataproc cluster pointing at the GCS location
    after DistCp.  If the metastore table is not available, we attempt a
    direct read using the Hive InputFormat.
    """
    path = _gcs_path(manifest)
    src = manifest["source"]
    db = src["database"]
    table = src["table"]

    logger.info("rcfile reader: path=%s, table=%s.%s", path, db, table)

    try:
        # Primary path: read from Hive metastore table registered on Dataproc
        # pointing to GCS location after DistCp
        df = spark.sql(f"SELECT * FROM {db}.{table}")
        logger.info("RCFile read via Hive metastore table %s.%s", db, table)
    except Exception as exc:
        logger.warning(
            "Hive metastore read failed for %s.%s (%s); "
            "falling back to Hive InputFormat reader",
            db, table, exc,
        )
        # Fallback: create a temp view using Hive InputFormat
        temp_view = f"_tmp_rcfile_{db}_{table}_{int(time.time())}"
        try:
            spark.sql(f"""
                CREATE TEMPORARY VIEW {temp_view}
                USING hive
                OPTIONS (
                    path '{path}',
                    fileFormat 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
                )
            """)
            df = spark.table(temp_view)
        except Exception as exc2:
            logger.error(
                "RCFile fallback also failed: %s. "
                "Ensure Hive SerDe JARs are on the classpath.", exc2,
            )
            raise RuntimeError(
                f"Cannot read RCFile table {db}.{table} from {path}"
            ) from exc2

    return df


# ---------------------------------------------------------------------------
# 3f. SequenceFile (supplier_invoices — custom Hadoop reader)
# ---------------------------------------------------------------------------
@_register_reader("sequencefile")
def _read_sequencefile(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read SequenceFile via Hive metastore or Hadoop InputFormat.

    SequenceFile key = invoice_no (Text), value = serialised row.
    Primary path: read from the Hive metastore table on Dataproc.
    Fallback: use sc.sequenceFile() and parse the value bytes.
    """
    path = _gcs_path(manifest)
    src = manifest["source"]
    db = src["database"]
    table = src["table"]

    logger.info("sequencefile reader: path=%s, table=%s.%s", path, db, table)

    try:
        # Primary: Hive metastore table registered on Dataproc pointing to GCS
        df = spark.sql(f"SELECT * FROM {db}.{table}")
        logger.info("SequenceFile read via Hive metastore table %s.%s", db, table)
    except Exception as exc:
        logger.warning(
            "Hive metastore read failed for %s.%s (%s); "
            "falling back to sc.sequenceFile() RDD reader",
            db, table, exc,
        )
        sc = spark.sparkContext
        rdd = sc.sequenceFile(
            path,
            keyClass="org.apache.hadoop.io.Text",
            valueClass="org.apache.hadoop.io.Text",
        )
        opts = manifest["source"].get("format_options", {})
        value_delimiter = opts.get("value_delimiter", "\t")

        # Column list for supplier_invoices (the only SequenceFile table)
        src_columns = [
            "invoice_no", "supplier_id", "invoice_date", "due_date",
            "total_amount", "currency", "line_items", "raw_xml",
        ]

        def _parse_seq_row(kv):
            _key, value = kv
            parts = value.split(value_delimiter)
            return {
                col: (parts[i] if i < len(parts) else None)
                for i, col in enumerate(src_columns)
            }

        parsed_rdd = rdd.map(_parse_seq_row)
        df = spark.createDataFrame(parsed_rdd)

    return df


# ---------------------------------------------------------------------------
# 3g. JSON SerDe / NDJSON (mobile_events, email_campaign_clicks, driver_logs)
# ---------------------------------------------------------------------------
@_register_reader("json_serde")
def _read_json_serde(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Newline-Delimited JSON (NDJSON) files.

    Hive JSON SerDe tables are stored as TEXTFILE with one JSON object per
    line.  Spark's native JSON reader handles this directly.
    """
    path = _gcs_path(manifest)
    logger.info("json_serde reader (NDJSON): path=%s", path)
    return (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )


# ---------------------------------------------------------------------------
# 3h. RegexSerDe (loyalty_events — pipe-delimited with TX:/META: tokens)
# ---------------------------------------------------------------------------
@_register_reader("regex_serde")
def _read_regex_serde(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read RegexSerDe tables via spark.read.text() + regexp_extract().

    Applies the same regex pattern from the Hive SerDe properties to extract
    named groups into separate columns.  For loyalty_events the pattern is:
    ``^([^|]+)\\|...\\|TX:([^;]+);META:(.*)$``
    """
    path = _gcs_path(manifest)
    opts = manifest["source"].get("format_options", {})
    pattern = opts["regex_pattern"]
    columns = opts["regex_columns"]

    logger.info(
        "regex_serde reader: path=%s, %d columns from regex", path, len(columns),
    )

    raw_df = spark.read.text(path)
    select_exprs = [
        F.regexp_extract(F.col("value"), pattern, idx).alias(col_name)
        for idx, col_name in enumerate(columns, start=1)
    ]
    return raw_df.select(*select_exprs)


# ---------------------------------------------------------------------------
# 3i. ORC — ACID tables (post-compaction base files)
# ---------------------------------------------------------------------------
@_register_reader("orc")
def _read_orc(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read ORC files — used for ACID tables after major compaction.

    Pre-migration step must have run ``ALTER TABLE ... COMPACT 'major'``
    so that delta files are merged into the base.  Spark then reads the
    ORC base files directly — no delta resolution needed.

    The ``source.acid_compaction`` flag in the manifest indicates this table
    requires the compaction pre-step (handled by the Composer DAG, not here).
    """
    path = _gcs_path(manifest)
    is_acid = manifest["source"].get("acid_compaction", False)
    logger.info(
        "orc reader: path=%s, acid_compaction=%s", path, is_acid,
    )
    return spark.read.orc(path)


# ---------------------------------------------------------------------------
# 3j. Kudu snapshot (exported as Parquet to GCS)
# ---------------------------------------------------------------------------
@_register_reader("kudu")
def _read_kudu(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Kudu table snapshot exported as Parquet to GCS.

    Per the locked kudu_realtime_migration decision, Kudu tables are
    exported as Parquet snapshots to GCS before loading into BigQuery.
    The epoch-ms → TIMESTAMP conversion is handled in the transformation
    phase, not here.
    """
    path = _gcs_path(manifest)
    logger.info("kudu reader (Parquet export): path=%s", path)
    return spark.read.parquet(path)


# ═══════════════════════════════════════════════════════════════════════════
# 4. Transformation Rules
# ═══════════════════════════════════════════════════════════════════════════

def apply_transforms(
    df: DataFrame,
    manifest: Dict[str, Any],
    watermark_ts: Optional[str],
) -> DataFrame:
    """Apply all in-flight transformation rules from the manifest.

    Rules are applied in deterministic order:
      0. Watermark filtering (bound data by frozen watermark W)
      2. Partition key conversion (STRING→DATE, multi-col INT→DATE)
      3. MAP → JSON conversion
      7. Kudu epoch ms → TIMESTAMP conversion
      *  Drop generated columns (BQ computes them; Spark must NOT write them)

    Rules handled natively by the spark-bigquery-connector (no code needed):
      4. ARRAY<STRUCT> / STRUCT pass-through
      5. Type widening (INT/TINYINT/SMALLINT→INT64, DOUBLE→FLOAT64, etc.)
    """
    transforms = manifest.get("transforms", {})

    # Rule 0: Watermark filtering
    df = _apply_watermark_filter(df, manifest, watermark_ts)

    # Rule 2: Partition key conversion
    df = _apply_partition_key_conversion(df, transforms)

    # Rule 3: MAP → JSON conversion
    df = _apply_map_to_json(df, transforms)

    # Rule 7: Kudu epoch ms → TIMESTAMP
    df = _apply_kudu_epoch_conversion(df, transforms)

    # Drop generated columns from output
    df = _drop_generated_columns(df, transforms)

    return df


# ---------------------------------------------------------------------------
# Rule 0: Watermark filtering
# ---------------------------------------------------------------------------
def _apply_watermark_filter(
    df: DataFrame,
    manifest: Dict[str, Any],
    watermark_ts: Optional[str],
) -> DataFrame:
    """Filter rows by the frozen watermark W.

    Only applies when both a ``watermark_ts`` is provided AND the manifest
    declares a ``validation.watermark_col``.  For tables partitioned by
    STRING date_ts, we compare the partition column lexicographically.
    For TIMESTAMP/DATE watermark columns we cast and compare.
    """
    if not watermark_ts:
        return df

    validation = manifest.get("validation", {})
    watermark_col = validation.get("watermark_col")
    if not watermark_col:
        logger.info("No watermark_col defined — loading full table")
        return df

    # Find the column type in the DataFrame
    col_type = None
    for field in df.schema.fields:
        if field.name == watermark_col:
            col_type = field.dataType
            break

    if col_type is None:
        # The watermark col might be a partition column not yet in the
        # DataFrame (read from Hive partition directory paths).
        logger.warning(
            "Watermark column '%s' not found in DataFrame schema — "
            "skipping filter",
            watermark_col,
        )
        return df

    # Parse watermark to a datetime
    try:
        wm_dt = datetime.fromisoformat(watermark_ts.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(
            "Cannot parse watermark '%s' as ISO datetime — skipping filter",
            watermark_ts,
        )
        return df

    if isinstance(col_type, T.StringType):
        # STRING partition keys (yyyyMMdd_HH or yyyyMMdd) —
        # lexicographic compare.  Append '_99' to include all hours
        # of the boundary date.
        wm_str = wm_dt.strftime("%Y%m%d")
        logger.info(
            "Applying STRING watermark filter: %s <= '%s_99'",
            watermark_col, wm_str,
        )
        df = df.filter(F.col(watermark_col) <= F.lit(wm_str + "_99"))

    elif isinstance(col_type, T.TimestampType):
        logger.info(
            "Applying TIMESTAMP watermark filter: %s <= '%s'",
            watermark_col, watermark_ts,
        )
        df = df.filter(
            F.col(watermark_col) <= F.lit(watermark_ts).cast("timestamp")
        )

    elif isinstance(col_type, T.DateType):
        wm_date_str = wm_dt.strftime("%Y-%m-%d")
        logger.info(
            "Applying DATE watermark filter: %s <= '%s'",
            watermark_col, wm_date_str,
        )
        df = df.filter(
            F.col(watermark_col) <= F.lit(wm_date_str).cast("date")
        )

    else:
        logger.warning(
            "Watermark column '%s' has unsupported type %s — skipping filter",
            watermark_col, col_type,
        )

    return df


# ---------------------------------------------------------------------------
# Rule 2: Partition key conversion
# ---------------------------------------------------------------------------
def _apply_partition_key_conversion(
    df: DataFrame,
    transforms: Dict[str, Any],
) -> DataFrame:
    """Convert Hive STRING/INT partition keys to BigQuery DATE columns.

    Patterns handled:
      - STRING yyyyMMdd_HH → partition_date DATE via to_date(substring, fmt)
        (only when generated_column=false)
      - Multi-column INT (year, month, day) → DATE(year, month, day)
        (only when generated_column=false)
      - Multi-column INT (year, month) → DATE(year, month, 1)
        (only when generated_column=false)
      - Native DATE → pass-through (no conversion needed)

    When ``generated_column=true``, BQ computes the partition column via a
    generated column expression in DDL.  Spark must NOT write it — this is
    handled by ``_drop_generated_columns``.
    """
    pk_config = transforms.get("partition_key_conversion")
    if not pk_config:
        return df

    is_generated = pk_config.get("generated_column", False)
    if is_generated:
        # Generated columns are computed by BQ DDL — nothing to add here.
        logger.info(
            "Partition column '%s' is a BQ generated column — "
            "skipping Spark derivation",
            pk_config.get("target_col", "?"),
        )
        return df

    target_col = pk_config["target_col"]
    parse_fn = pk_config.get("parse_fn", "PARSE_DATE")

    if parse_fn == "PARSE_DATE":
        # STRING → DATE conversion
        source_col = pk_config.get("source_col")
        if not source_col or source_col not in df.columns:
            logger.warning(
                "Partition source column '%s' not in DataFrame — skipping",
                source_col,
            )
            return df

        # STRING yyyyMMdd_HH: take first 8 chars for the date portion
        # STRING yyyyMMdd: use as-is (substring still safe)
        logger.info(
            "Partition key conversion: %s (STRING) → %s (DATE) "
            "via to_date(substring(col, 1, 8), 'yyyyMMdd')",
            source_col, target_col,
        )
        df = df.withColumn(
            target_col,
            F.to_date(F.substring(F.col(source_col), 1, 8), "yyyyMMdd"),
        )

    elif parse_fn == "DATE":
        # Multi-column INT → DATE conversion
        source_cols = pk_config.get("source_cols", [])
        if len(source_cols) == 3:
            logger.info(
                "Partition key conversion: %s → %s (DATE) via make_date(y,m,d)",
                source_cols, target_col,
            )
            df = df.withColumn(
                target_col,
                F.make_date(
                    F.col(source_cols[0]).cast("int"),
                    F.col(source_cols[1]).cast("int"),
                    F.col(source_cols[2]).cast("int"),
                ),
            )
        elif len(source_cols) == 2:
            logger.info(
                "Partition key conversion: %s → %s (DATE) via make_date(y,m,1)",
                source_cols, target_col,
            )
            df = df.withColumn(
                target_col,
                F.make_date(
                    F.col(source_cols[0]).cast("int"),
                    F.col(source_cols[1]).cast("int"),
                    F.lit(1),
                ),
            )
        else:
            logger.warning(
                "DATE parse_fn requires 2 or 3 source_cols, got %d — skipping",
                len(source_cols),
            )

    else:
        logger.warning(
            "Unknown parse_fn '%s' — skipping partition conversion", parse_fn,
        )

    return df


# ---------------------------------------------------------------------------
# Rule 3: MAP<STRING,STRING> → JSON conversion
# ---------------------------------------------------------------------------
def _apply_map_to_json(
    df: DataFrame,
    transforms: Dict[str, Any],
) -> DataFrame:
    """Convert Hive MAP<STRING,STRING> columns to JSON strings.

    Null handling per locked decision:
      - Hive MAP NULL  → BigQuery JSON NULL  (preserved by to_json)
      - Empty MAP {}   → JSON '{}'           (preserved by to_json)
      - NULL values     → JSON '{"key":null}' (preserved by to_json)

    Spark's ``to_json()`` handles all three cases correctly out of the box.
    """
    map_cols = transforms.get("map_to_json", [])
    if not map_cols:
        return df

    for mapping in map_cols:
        source_col = mapping["source_col"]
        target_col = mapping.get("target_col", source_col)

        if source_col not in df.columns:
            logger.warning(
                "MAP column '%s' not in DataFrame — skipping JSON conversion",
                source_col,
            )
            continue

        field_type = df.schema[source_col].dataType

        if isinstance(field_type, T.MapType):
            logger.info(
                "MAP→JSON: %s (MapType) → %s (JSON string)", source_col, target_col,
            )
            if source_col == target_col:
                df = df.withColumn(target_col, F.to_json(F.col(source_col)))
            else:
                df = (
                    df
                    .withColumn(target_col, F.to_json(F.col(source_col)))
                    .drop(source_col)
                )
        else:
            # Column might already be a string (e.g. from JSON reader) —
            # pass through as-is; just rename if needed.
            logger.info(
                "Column '%s' is %s (not MapType) — passing through as-is",
                source_col, field_type,
            )
            if source_col != target_col:
                df = df.withColumnRenamed(source_col, target_col)

    return df


# ---------------------------------------------------------------------------
# Rule 7: Kudu epoch milliseconds → TIMESTAMP
# ---------------------------------------------------------------------------
def _apply_kudu_epoch_conversion(
    df: DataFrame,
    transforms: Dict[str, Any],
) -> DataFrame:
    """Convert Kudu BIGINT epoch-ms columns to Spark TimestampType.

    Kudu stores timestamps as INT64 milliseconds since epoch.  BigQuery
    target expects TIMESTAMP.  The manifest lists these under
    ``transforms.kudu_epoch_conversion`` as a list of
    ``{col: <name>, from_unit: millis}``.
    """
    kudu_cols = transforms.get("kudu_epoch_conversion", [])
    if not kudu_cols:
        return df

    for entry in kudu_cols:
        col_name = entry["col"]
        from_unit = entry.get("from_unit", "millis")

        if col_name not in df.columns:
            logger.warning(
                "Kudu epoch column '%s' not in DataFrame — skipping", col_name,
            )
            continue

        logger.info(
            "Kudu epoch conversion: %s (BIGINT %s) → TIMESTAMP",
            col_name, from_unit,
        )

        if from_unit == "millis":
            # milliseconds → seconds → timestamp
            df = df.withColumn(
                col_name,
                (F.col(col_name) / 1000).cast("timestamp"),
            )
        elif from_unit == "seconds":
            df = df.withColumn(
                col_name,
                F.col(col_name).cast("timestamp"),
            )
        elif from_unit == "micros":
            df = df.withColumn(
                col_name,
                (F.col(col_name) / 1_000_000).cast("timestamp"),
            )
        else:
            logger.warning(
                "Unknown from_unit '%s' for Kudu epoch column '%s'",
                from_unit, col_name,
            )

    return df


# ---------------------------------------------------------------------------
# Drop generated columns
# ---------------------------------------------------------------------------
def _drop_generated_columns(
    df: DataFrame,
    transforms: Dict[str, Any],
) -> DataFrame:
    """Remove columns that BQ will auto-compute via generated column expressions.

    When the manifest says ``generated_column: true`` for a partition key,
    Spark must NOT include that column in the output — BigQuery will compute
    it from the source INT/STRING columns via the DDL expression.
    """
    pk_config = transforms.get("partition_key_conversion")
    if not pk_config:
        return df

    if not pk_config.get("generated_column", False):
        return df

    target_col = pk_config.get("target_col")
    if target_col and target_col in df.columns:
        logger.info(
            "Dropping generated column '%s' from output — BQ will compute it",
            target_col,
        )
        df = df.drop(target_col)

    return df


# ═══════════════════════════════════════════════════════════════════════════
# 5. BigQuery Writer
# ═══════════════════════════════════════════════════════════════════════════

def write_to_bigquery(
    df: DataFrame,
    manifest: Dict[str, Any],
    gcs_staging_bucket: str,
) -> int:
    """Write the transformed DataFrame to BigQuery via Storage Write API.

    Uses ``writeMethod=direct`` for exactly-once committed mode.
    The spark-bigquery-connector handles type mapping natively:
      - Spark IntegerType / LongType → BQ INT64
      - Spark StringType             → BQ STRING
      - Spark DecimalType(p,s)       → BQ NUMERIC(p,s)
      - Spark ArrayType(StructType)  → BQ ARRAY<STRUCT>
      - Spark StructType             → BQ STRUCT
      - Spark TimestampType          → BQ TIMESTAMP
      - Spark DateType               → BQ DATE
      - Spark BooleanType            → BQ BOOL
      - Spark DoubleType / FloatType → BQ FLOAT64
      - String columns → BQ JSON (when target column type is JSON)

    Returns the number of rows written.
    """
    target = manifest["target"]
    bq_table = f"{target['project']}.{target['dataset']}.{target['table']}"

    row_count = df.count()
    logger.info("Writing %d rows to BigQuery table: %s", row_count, bq_table)

    if row_count == 0:
        logger.warning("DataFrame is empty — nothing to write for %s", bq_table)
        return 0

    write_options = {
        "table": bq_table,
        "writeMethod": "direct",
        "temporaryGcsBucket": gcs_staging_bucket,
        "allowFieldRelaxation": "true",
    }

    # Bulk historical load always uses overwrite mode
    df.write.format("bigquery").options(**write_options).mode("overwrite").save()

    logger.info("Successfully wrote %d rows to %s", row_count, bq_table)
    return row_count


# ═══════════════════════════════════════════════════════════════════════════
# 6. CLI Argument Parser
# ═══════════════════════════════════════════════════════════════════════════

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Arguments
    ---------
    --manifest-path : str
        Path to the YAML manifest for the table being loaded.
        Supports both local paths and ``gs://`` URIs.
    --watermark-ts : str, optional
        Frozen watermark timestamp (ISO-8601).  All loaded data will be
        bounded to rows at or before this timestamp.
    --gcs-staging-bucket : str
        GCS bucket for BigQuery connector temporary staging files.
    --dry-run : flag
        If set, read and transform data but do not write to BigQuery.
        Prints row count and schema instead.
    """
    parser = argparse.ArgumentParser(
        description="Config-driven bulk migration: GCS → BigQuery",
    )
    parser.add_argument(
        "--manifest-path",
        required=True,
        help="Path to the YAML table manifest (local or gs://)",
    )
    parser.add_argument(
        "--watermark-ts",
        default=None,
        help="Frozen watermark timestamp (ISO-8601). Optional.",
    )
    parser.add_argument(
        "--gcs-staging-bucket",
        required=True,
        help="GCS bucket for BQ connector temporary files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Read and transform only — do not write to BigQuery",
    )
    return parser.parse_args(argv)


# ═══════════════════════════════════════════════════════════════════════════
# 7. Main Entrypoint
# ═══════════════════════════════════════════════════════════════════════════

def main(argv: Optional[List[str]] = None) -> None:
    """Main entry point for the bulk load pipeline.

    Orchestration flow:
      1. Parse CLI args
      2. Load YAML manifest
      3. Build SparkSession
      4. Read source data (format-specific handler)
      5. Apply in-flight transformations
      6. Write to BigQuery (or dry-run)
      7. Log summary
    """
    args = parse_args(argv)

    # Step 1-2: Load manifest (bootstrap a Spark session for GCS paths)
    if args.manifest_path.startswith("gs://"):
        tmp_spark = SparkSession.builder.appName("manifest_loader").getOrCreate()
        manifest = load_manifest(args.manifest_path, spark=tmp_spark)
        tmp_spark.stop()
    else:
        manifest = load_manifest(args.manifest_path)

    src = manifest["source"]
    tgt = manifest["target"]
    table_fqn = f"{src['database']}.{src['table']}"

    logger.info("=" * 72)
    logger.info("BULK LOAD START: %s", table_fqn)
    logger.info("  Format      : %s", src["format"])
    logger.info("  GCS path    : %s", src.get("gcs_path", "N/A"))
    logger.info("  ACID compact: %s", src.get("acid_compaction", False))
    logger.info("  Target      : %s.%s.%s", tgt["project"], tgt["dataset"], tgt["table"])
    logger.info("  Wave        : %s", manifest.get("wave", "unassigned"))
    logger.info("  Watermark   : %s", args.watermark_ts or "NONE (full load)")
    logger.info("=" * 72)

    pipeline_start = time.time()

    # Step 3: Build SparkSession
    spark = build_spark_session(manifest, args.gcs_staging_bucket)
    logger.info("SparkSession created: %s", spark.sparkContext.applicationId)

    try:
        # Step 4: Read source data
        read_start = time.time()
        df = read_source(spark, manifest)
        read_elapsed = time.time() - read_start
        logger.info(
            "Source read complete: %d columns, %.1fs elapsed",
            len(df.columns), read_elapsed,
        )
        logger.info("Source schema:\n%s", df._jdf.schema().treeString())

        # Step 5: Apply transformations
        transform_start = time.time()
        df = apply_transforms(df, manifest, args.watermark_ts)
        transform_elapsed = time.time() - transform_start
        logger.info("Transforms applied: %.1fs elapsed", transform_elapsed)
        logger.info("Output schema:\n%s", df._jdf.schema().treeString())

        # Step 6: Write to BigQuery (or dry-run)
        write_elapsed = 0.0
        if args.dry_run:
            row_count = df.count()
            logger.info("DRY RUN — would write %d rows", row_count)
            logger.info("Output columns: %s", df.columns)
            df.printSchema()
            df.show(5, truncate=40)
        else:
            write_start = time.time()
            row_count = write_to_bigquery(df, manifest, args.gcs_staging_bucket)
            write_elapsed = time.time() - write_start
            logger.info(
                "BigQuery write complete: %d rows, %.1fs elapsed",
                row_count, write_elapsed,
            )

        # Step 7: Summary
        total_elapsed = time.time() - pipeline_start
        logger.info("=" * 72)
        logger.info("BULK LOAD COMPLETE: %s", table_fqn)
        logger.info("  Rows written  : %d", row_count)
        logger.info("  Read time     : %.1fs", read_elapsed)
        logger.info("  Transform time: %.1fs", transform_elapsed)
        if not args.dry_run:
            logger.info("  Write time    : %.1fs", write_elapsed)
        logger.info("  Total time    : %.1fs", total_elapsed)
        logger.info("=" * 72)

    except Exception:
        logger.exception("BULK LOAD FAILED: %s", table_fqn)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
