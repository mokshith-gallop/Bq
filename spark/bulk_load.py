#!/usr/bin/env python3
"""
bulk_load.py — Config-driven PySpark bulk migration job.

Reads a per-table YAML manifest, extracts data from GCS (DistCp'd HDFS copy),
applies in-flight transformations, and writes to BigQuery via the
spark-bigquery-connector (Storage Write API, writeMethod=direct).

Usage (Dataproc Serverless):
    gcloud dataproc batches submit pyspark spark/bulk_load.py -- \
        --manifest-path gs://acme-migration-staging-us/config/tables/raw/sales_retail.yaml \
        --watermark-ts '2024-06-01T00:00:00Z' \
        --gcs-staging-bucket acme-migration-staging-us

Usage (local / Dataproc cluster):
    spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
        spark/bulk_load.py \
        --manifest-path config/tables/raw/sales_retail.yaml \
        --watermark-ts '2024-06-01T00:00:00Z' \
        --gcs-staging-bucket acme-migration-staging-us
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        jvm = spark.sparkContext._jvm  # type: ignore[union-attr]
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[union-attr]
        gcs_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = gcs_path.getFileSystem(hadoop_conf)
        stream = fs.open(gcs_path)
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream))
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
    _validate_manifest(manifest)
    return manifest


def _validate_manifest(manifest: Dict[str, Any]) -> None:
    """Validate that required manifest fields are present."""
    required_top = ["source", "target"]
    for key in required_top:
        if key not in manifest:
            raise ValueError(f"Manifest missing required key: '{key}'")
    required_source = ["database", "table", "format"]
    for key in required_source:
        if key not in manifest["source"]:
            raise ValueError(f"Manifest source section missing required key: '{key}'")
    required_target = ["project", "dataset", "table"]
    for key in required_target:
        if key not in manifest["target"]:
            raise ValueError(f"Manifest target section missing required key: '{key}'")


# ═══════════════════════════════════════════════════════════════════════════
# 2. SparkSession Builder
# ═══════════════════════════════════════════════════════════════════════════

def build_spark_session(manifest: Dict[str, Any], gcs_staging_bucket: str) -> SparkSession:
    """Create a SparkSession configured for BigQuery writes and adaptive execution."""
    table_name = f"{manifest['source']['database']}.{manifest['source']['table']}"
    wave = manifest.get("wave", "unknown")

    builder = (
        SparkSession.builder
        .appName(f"bulk_load__{table_name}__{wave}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # BigQuery connector settings
        .config("spark.datasource.bigquery.writeMethod", "direct")
        .config("spark.datasource.bigquery.temporaryGcsBucket", gcs_staging_bucket)
        .config("spark.datasource.bigquery.writeAtLeastOneRecord", "false")
    )

    # For large tables (Wave 3) — increase parallelism
    if wave == "wave_3_large":
        builder = builder.config("spark.sql.shuffle.partitions", "200")

    src_format = manifest["source"]["format"]
    # RCFile and SequenceFile need Hive support
    if src_format in ("rcfile", "sequencefile"):
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()


# ═══════════════════════════════════════════════════════════════════════════
# 3. Format-Specific Readers
# ═══════════════════════════════════════════════════════════════════════════

class FormatReaderRegistry:
    """Dispatch table for format-specific read handlers.

    Each handler receives ``(spark, manifest)`` and returns a DataFrame
    containing the raw data (before transformations).
    """

    _registry: Dict[str, Any] = {}

    @classmethod
    def register(cls, format_name: str):
        """Decorator to register a reader function for a given format."""
        def decorator(fn):
            cls._registry[format_name] = fn
            return fn
        return decorator

    @classmethod
    def read(cls, spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
        fmt = manifest["source"]["format"]
        reader_fn = cls._registry.get(fmt)
        if reader_fn is None:
            raise ValueError(
                f"No reader registered for format '{fmt}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        logger.info("Reading source data with format handler: %s", fmt)
        return reader_fn(spark, manifest)


def _get_gcs_path(manifest: Dict[str, Any]) -> str:
    """Return the GCS path for the source data."""
    return manifest["source"]["gcs_path"]


# ---------------------------------------------------------------------------
# 3a. TEXTFILE CSV (comma-delimited with header)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("textfile_csv")
def _read_textfile_csv(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read comma-separated TEXTFILE tables.

    Handles skip.header.line.count=1 via ``header=True`` and
    serialization.null.format='' via ``nullValue=''``.
    """
    gcs_path = _get_gcs_path(manifest)
    opts = manifest["source"].get("format_options", {})
    delimiter = opts.get("delimiter", ",")
    header = opts.get("header", True)
    null_format = opts.get("null_format", "")

    logger.info(
        "textfile_csv reader: path=%s, delimiter=%r, header=%s",
        gcs_path, delimiter, header,
    )

    df = (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", "false")
        .option("nullValue", null_format)
        .option("emptyValue", null_format)
        .csv(gcs_path, sep=delimiter)
    )
    return df


# ---------------------------------------------------------------------------
# 3b. TEXTFILE TSV (tab-delimited, no header — e.g. omniture_logs)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("textfile_tsv")
def _read_textfile_tsv(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read tab-separated TEXTFILE tables (no header line).

    Used for omniture_logs: 60 STRING columns, no header, tab-separated.
    """
    gcs_path = _get_gcs_path(manifest)
    opts = manifest["source"].get("format_options", {})
    header = opts.get("header", False)

    logger.info("textfile_tsv reader: path=%s, header=%s", gcs_path, header)

    df = (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", "false")
        .option("sep", "\t")
        .csv(gcs_path)
    )
    return df


# ---------------------------------------------------------------------------
# 3c. Parquet (native columnar — most tables)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("parquet")
def _read_parquet(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Parquet files — native, no conversion needed."""
    gcs_path = _get_gcs_path(manifest)
    logger.info("parquet reader: path=%s", gcs_path)
    return spark.read.parquet(gcs_path)


# ---------------------------------------------------------------------------
# 3d. Avro (customer_signups, fraud_signals)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("avro")
def _read_avro(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Avro files.  Requires spark-avro package on the Dataproc image."""
    gcs_path = _get_gcs_path(manifest)
    logger.info("avro reader: path=%s", gcs_path)
    return spark.read.format("avro").load(gcs_path)


# ---------------------------------------------------------------------------
# 3e. RCFile (product_catalog_feed — via Hive compatibility layer)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("rcfile")
def _read_rcfile(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read RCFile via Hive's SerDe compatibility layer.

    Spark must be launched with Hive support enabled and the Hive SerDe JARs
    available on the classpath.  We create a temporary Hive external table
    pointing at the GCS location, read it, then drop the temp table.
    """
    gcs_path = _get_gcs_path(manifest)
    src = manifest["source"]
    db = src["database"]
    table = src["table"]
    temp_table = f"_tmp_rcfile_{db}_{table}_{int(time.time())}"

    logger.info("rcfile reader: path=%s (temp Hive table %s)", gcs_path, temp_table)

    # Build column definitions from the target schema as a proxy.
    # In production the Hive metastore on Dataproc provides the schema;
    # here we use Hive-format DDL.  The actual column list comes from the
    # source Hive DDL replicated on the Dataproc metastore.
    spark.sql(f"""
        CREATE TEMPORARY VIEW {temp_table}
        USING org.apache.spark.sql.hive.HiveRCFileFormat
        OPTIONS (
            path '{gcs_path}'
        )
    """)

    try:
        # Fallback: read via the Hive SerDe pathway
        # If the TEMPORARY VIEW approach above fails (no HiveRCFileFormat),
        # we try reading via the registered Hive table on the metastore.
        df = spark.table(temp_table)
    except Exception:
        logger.warning(
            "HiveRCFileFormat view failed; falling back to Hive metastore read "
            "for %s.%s", db, table,
        )
        # Read from the Hive metastore table that references the GCS path
        # (assumes Dataproc metastore is configured with the migrated table).
        df = spark.sql(f"SELECT * FROM {db}.{table}")

    return df


# ---------------------------------------------------------------------------
# 3f. SequenceFile (supplier_invoices — custom Hadoop reader)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("sequencefile")
def _read_sequencefile(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read SequenceFile via Hadoop InputFormat.

    SequenceFile key = invoice_no (Text), value = serialised row.
    We use the Hive SequenceFile InputFormat to parse the value bytes
    through the Hive SerDe, similar to RCFile.
    """
    gcs_path = _get_gcs_path(manifest)
    src = manifest["source"]
    db = src["database"]
    table = src["table"]

    logger.info("sequencefile reader: path=%s", gcs_path)

    try:
        # Preferred path: read via Hive metastore table registered on Dataproc.
        # The Dataproc metastore should have this table created via HQL pointing
        # to the GCS location after DistCp.
        df = spark.sql(f"SELECT * FROM {db}.{table}")
    except Exception:
        logger.warning(
            "Hive metastore read failed for %s.%s; falling back to "
            "sc.sequenceFile() RDD reader",
            db, table,
        )
        # Fallback: low-level sequenceFile reader.
        # Key=Text, Value=Text (Hive SequenceFile SerDe serialises values as Text).
        sc = spark.sparkContext
        rdd = sc.sequenceFile(
            gcs_path,
            keyClass="org.apache.hadoop.io.Text",
            valueClass="org.apache.hadoop.io.Text",
        )
        # Parse pipe-delimited value string into columns.
        # The column order matches the Hive DDL (excluding partition cols).
        opts = manifest["source"].get("format_options", {})
        value_delimiter = opts.get("value_delimiter", "\t")

        src_columns = [
            "invoice_no", "supplier_id", "invoice_date", "due_date",
            "total_amount", "currency", "line_items", "raw_xml",
        ]

        def _parse_seq_row(kv):
            key, value = kv
            parts = value.split(value_delimiter)
            row = {}
            for i, col_name in enumerate(src_columns):
                row[col_name] = parts[i] if i < len(parts) else None
            return row

        parsed_rdd = rdd.map(_parse_seq_row)
        df = spark.createDataFrame(parsed_rdd)

    return df


# ---------------------------------------------------------------------------
# 3g. JSON SerDe / NDJSON (mobile_events, email_campaign_clicks, driver_logs)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("json_serde")
def _read_json_serde(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Newline-Delimited JSON (NDJSON) files.

    Hive JSON SerDe tables are stored as TEXTFILE with one JSON object per
    line.  Spark's native JSON reader handles this directly.
    """
    gcs_path = _get_gcs_path(manifest)
    logger.info("json_serde reader (NDJSON): path=%s", gcs_path)

    df = (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(gcs_path)
    )
    return df


# ---------------------------------------------------------------------------
# 3h. RegexSerDe (loyalty_events — pipe-delimited with TX:/META: tokens)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("regex_serde")
def _read_regex_serde(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read RegexSerDe tables via spark.read.text() + regexp_extract().

    Applies the same regex pattern from the Hive SerDe properties to extract
    named groups into separate columns.
    """
    gcs_path = _get_gcs_path(manifest)
    opts = manifest["source"].get("format_options", {})
    pattern = opts["regex_pattern"]
    columns = opts["regex_columns"]

    logger.info(
        "regex_serde reader: path=%s, pattern=%s, columns=%s",
        gcs_path, pattern, columns,
    )

    raw_df = spark.read.text(gcs_path)

    select_exprs = []
    for idx, col_name in enumerate(columns, start=1):
        select_exprs.append(
            F.regexp_extract(F.col("value"), pattern, idx).alias(col_name)
        )

    parsed_df = raw_df.select(*select_exprs)
    return parsed_df


# ---------------------------------------------------------------------------
# 3i. ORC — ACID tables (post-compaction base files)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("orc_acid")
def _read_orc_acid(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read compacted ORC base files from ACID Hive tables.

    Pre-migration step must have run ``ALTER TABLE ... COMPACT 'major'``
    so that delta files are merged into the base.  Spark then reads the
    ORC base files directly — no delta resolution needed.
    """
    gcs_path = _get_gcs_path(manifest)
    logger.info("orc_acid reader (compacted base): path=%s", gcs_path)
    return spark.read.orc(gcs_path)


# ---------------------------------------------------------------------------
# 3j. Kudu snapshot (via Parquet export or Kudu connector)
# ---------------------------------------------------------------------------
@FormatReaderRegistry.register("kudu_snapshot")
def _read_kudu_snapshot(spark: SparkSession, manifest: Dict[str, Any]) -> DataFrame:
    """Read Kudu table snapshot exported as Parquet to GCS.

    Per the locked kudu_realtime_migration decision, Kudu tables are exported
    as Parquet snapshots to GCS before loading into BigQuery.
    """
    gcs_path = _get_gcs_path(manifest)
    logger.info("kudu_snapshot reader (Parquet export): path=%s", gcs_path)
    return spark.read.parquet(gcs_path)


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
      1. Watermark filtering (bound data by frozen watermark W)
      2. Partition key conversion (STRING→DATE, multi-col INT→DATE)
      3. MAP → JSON conversion
      4. Kudu epoch ms → TIMESTAMP conversion
      5. Type widening is handled natively by the spark-bigquery-connector
      6. ARRAY<STRUCT> / STRUCT pass-through (no transform needed)
      7. Drop generated columns (BQ computes them; Spark must NOT write them)
    """
    transforms = manifest.get("transforms", {})

    # --- Rule 0: Watermark filtering ----------------------------------
    df = _apply_watermark_filter(df, manifest, watermark_ts)

    # --- Rule 2: Partition key conversion -----------------------------
    df = _apply_partition_key_conversion(df, transforms)

    # --- Rule 3: MAP → JSON conversion --------------------------------
    df = _apply_map_to_json(df, transforms)

    # --- Rule 7: Kudu epoch ms → TIMESTAMP ----------------------------
    df = _apply_kudu_epoch_conversion(df, transforms)

    # --- Drop generated columns from the output -----------------------
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
    For TIMESTAMP watermark columns we cast and compare.
    """
    if not watermark_ts:
        return df

    validation = manifest.get("validation", {})
    watermark_col = validation.get("watermark_col")
    if not watermark_col:
        logger.info("No watermark_col defined — loading full table")
        return df

    # Determine column type in the DataFrame
    col_type = None
    for field in df.schema.fields:
        if field.name == watermark_col:
            col_type = field.dataType
            break

    if col_type is None:
        # The watermark col might be a partition column not yet in the DataFrame
        # (e.g. read from partition directory paths).  Skip filtering.
        logger.warning(
            "Watermark column '%s' not found in DataFrame schema — skipping filter",
            watermark_col,
        )
        return df

    # STRING partition keys (yyyyMMdd_HH or yyyyMMdd) — lexicographic compare
    if isinstance(col_type, T.StringType):
        # Extract date portion of watermark for string comparison
        # Watermark format: '2024-06-01T00:00:00Z' → '20240601'
        try:
            wm_dt = datetime.fromisoformat(watermark_ts.replace("Z", "+00:00"))
            wm_str = wm_dt.strftime("%Y%m%d")
        except ValueError:
            wm_str = watermark_ts
        logger.info(
            "Applying STRING watermark filter: %s <= '%s'", watermark_col, wm_str,
        )
        # yyyyMMdd_HH format: '20240601_00' <= '20240601' + '_99' ensures
        # we capture all hours of the boundary date.
        df = df.filter(
            F.col(watermark_col) <= F.lit(wm_str + "_99")
        )
    elif isinstance(col_type, T.TimestampType):
        logger.info(
            "Applying TIMESTAMP watermark filter: %s <= '%s'",
            watermark_col, watermark_ts,
        )
        df = df.filter(F.col(watermark_col) <= F.lit(watermark_ts).cast("timestamp"))
    elif isinstance(col_type, T.DateType):
        wm_dt = datetime.fromisoformat(watermark_ts.replace("Z", "+00:00"))
        wm_date_str = wm_dt.strftime("%Y-%m-%d")
        logger.info(
            "Applying DATE watermark filter: %s <= '%s'", watermark_col, wm_date_str,
        )
        df = df.filter(F.col(watermark_col) <= F.lit(wm_date_str).cast("date"))
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
      - STRING yyyyMMdd_HH → PARSE_DATE('%Y%m%d', SUBSTRING(date_ts, 1, 8))
        → partition_date DATE  (only when generated_column=false)
      - Multi-column INT (year, month, day) → DATE(year, month, day)
        → partition_date DATE  (only when generated_column=false)
      - Multi-column INT (year, month) → DATE(year, month, 1)
        → partition_month DATE  (only when generated_column=false)
      - Native DATE → pass-through (no conversion)

    When ``generated_column=true``, BQ computes the partition column via a
    generated column expression in DDL.  Spark must NOT write it.
    """
    pk_config = transforms.get("partition_key_conversion")
    if not pk_config:
        return df

    is_generated = pk_config.get("generated_column", False)
    if is_generated:
        # Generated columns are handled by BQ DDL — nothing to do.
        logger.info(
            "Partition column '%s' is a BQ generated column — skipping Spark derivation",
            pk_config.get("target_col", "?"),
        )
        return df

    target_col = pk_config["target_col"]
    parse_fn = pk_config.get("parse_fn", "PARSE_DATE")

    if parse_fn == "PARSE_DATE":
        # STRING → DATE conversion
        source_col = pk_config["source_col"]
        parse_format = pk_config.get("parse_format", "%Y%m%d")

        if source_col not in df.columns:
            logger.warning(
                "Partition source column '%s' not in DataFrame — skipping", source_col,
            )
            return df

        # STRING yyyyMMdd_HH: take first 8 chars for the date portion
        # STRING yyyyMMdd: use as-is
        logger.info(
            "Partition key conversion: %s (STRING) → %s (DATE) via PARSE_DATE",
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
            # year, month, day → DATE(year, month, day)
            logger.info(
                "Partition key conversion: %s → %s (DATE) via make_date",
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
            # year, month → DATE(year, month, 1)
            logger.info(
                "Partition key conversion: %s → %s (DATE) via make_date(y, m, 1)",
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
        logger.warning("Unknown parse_fn '%s' — skipping partition conversion", parse_fn)

    return df


# ---------------------------------------------------------------------------
# Rule 3: MAP<STRING,STRING> → JSON conversion
# ---------------------------------------------------------------------------
def _apply_map_to_json(
    df: DataFrame,
    transforms: Dict[str, Any],
) -> DataFrame:
    """Convert Hive MAP<STRING,STRING> columns to JSON strings.

    Null handling per decision:
      - Hive MAP NULL  → BigQuery JSON NULL  (preserved)
      - Empty MAP {}   → JSON '{}'           (not NULL)
      - NULL values in MAP → JSON '{"key": null}'

    ``to_json()`` in Spark handles all three cases correctly:
      - NULL MapType → NULL output
      - Empty MapType → '{}'
      - MapType with null values → '{"key":null}'
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

        # Check if the column is actually a MapType
        field_type = df.schema[source_col].dataType
        if isinstance(field_type, T.MapType):
            logger.info(
                "MAP→JSON conversion: %s (MapType) → %s (JSON string)",
                source_col, target_col,
            )
            if source_col == target_col:
                # In-place replacement
                df = df.withColumn(target_col, F.to_json(F.col(source_col)))
            else:
                df = (
                    df
                    .withColumn(target_col, F.to_json(F.col(source_col)))
                    .drop(source_col)
                )
        else:
            # Column might already be a string (e.g. from JSON reader) —
            # pass through as-is.
            logger.info(
                "Column '%s' is %s (not MapType) — assuming already JSON-compatible",
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
    target expects TIMESTAMP.
    """
    kudu_cols = transforms.get("kudu_epoch_cols", [])
    if not kudu_cols:
        return df

    for col_name in kudu_cols:
        if col_name not in df.columns:
            logger.warning(
                "Kudu epoch column '%s' not in DataFrame — skipping", col_name,
            )
            continue

        logger.info(
            "Kudu epoch conversion: %s (BIGINT ms) → TIMESTAMP", col_name,
        )
        # Convert milliseconds to seconds, then to timestamp.
        # Spark's from_unixtime returns STRING; cast to TIMESTAMP.
        df = df.withColumn(
            col_name,
            (F.col(col_name) / 1000).cast("timestamp"),
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
    it from the source INT/STRING columns.
    """
    pk_config = transforms.get("partition_key_conversion")
    if not pk_config:
        return df

    is_generated = pk_config.get("generated_column", False)
    if not is_generated:
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
    The spark-bigquery-connector handles type mapping:
      - Spark IntegerType/LongType → BQ INT64
      - Spark StringType → BQ STRING
      - Spark DecimalType → BQ NUMERIC
      - Spark ArrayType(StructType) → BQ ARRAY<STRUCT>
      - Spark StructType → BQ STRUCT
      - Spark TimestampType → BQ TIMESTAMP
      - Spark DateType → BQ DATE
      - Spark BooleanType → BQ BOOL
      - Spark DoubleType → BQ FLOAT64
      - String columns containing JSON → BQ JSON (when target column type is JSON)

    Returns the number of rows written.
    """
    target = manifest["target"]
    bq_table = f"{target['project']}.{target['dataset']}.{target['table']}"

    row_count = df.count()
    logger.info(
        "Writing %d rows to BigQuery table: %s", row_count, bq_table,
    )

    if row_count == 0:
        logger.warning("DataFrame is empty — nothing to write for %s", bq_table)
        return 0

    # Build write options
    write_options = {
        "table": bq_table,
        "writeMethod": "direct",
        "temporaryGcsBucket": gcs_staging_bucket,
        # Allow schema evolution for minor mismatches (e.g. NULLABLE vs REQUIRED)
        "allowFieldRelaxation": "true",
    }

    # Determine write mode — OVERWRITE for bulk historical load
    write_mode = manifest.get("write_mode", "overwrite")

    df.write.format("bigquery").options(**write_options).mode(write_mode).save()

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

    # Step 1-2: Load manifest (need a temporary Spark session for GCS paths)
    if args.manifest_path.startswith("gs://"):
        # Bootstrap a minimal session to read the manifest
        tmp_spark = SparkSession.builder.appName("manifest_loader").getOrCreate()
        manifest = load_manifest(args.manifest_path, spark=tmp_spark)
        tmp_spark.stop()
    else:
        manifest = load_manifest(args.manifest_path)

    table_fqn = f"{manifest['source']['database']}.{manifest['source']['table']}"
    logger.info("=" * 70)
    logger.info("BULK LOAD START: %s", table_fqn)
    logger.info("  Format     : %s", manifest["source"]["format"])
    logger.info("  GCS path   : %s", manifest["source"].get("gcs_path", "N/A"))
    logger.info("  Target     : %s.%s.%s",
                manifest["target"]["project"],
                manifest["target"]["dataset"],
                manifest["target"]["table"])
    logger.info("  Wave       : %s", manifest.get("wave", "unassigned"))
    logger.info("  Watermark  : %s", args.watermark_ts or "NONE (full load)")
    logger.info("=" * 70)

    pipeline_start = time.time()

    # Step 3: Build SparkSession
    spark = build_spark_session(manifest, args.gcs_staging_bucket)
    logger.info("SparkSession created: %s", spark.sparkContext.applicationId)

    try:
        # Step 4: Read source data
        read_start = time.time()
        df = FormatReaderRegistry.read(spark, manifest)
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
            logger.info("BigQuery write complete: %d rows, %.1fs elapsed",
                        row_count, write_elapsed)

        # Step 7: Summary
        total_elapsed = time.time() - pipeline_start
        logger.info("=" * 70)
        logger.info("BULK LOAD COMPLETE: %s", table_fqn)
        logger.info("  Rows written  : %d", row_count)
        logger.info("  Read time     : %.1fs", read_elapsed)
        logger.info("  Transform time: %.1fs", transform_elapsed)
        if not args.dry_run:
            logger.info("  Write time    : %.1fs", write_elapsed)
        logger.info("  Total time    : %.1fs", total_elapsed)
        logger.info("=" * 70)

    except Exception:
        logger.exception("BULK LOAD FAILED: %s", table_fqn)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
