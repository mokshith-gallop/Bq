# Locked Decisions for Story ebcfef06-8c46-43f8-83df-9ec3a1190f46

## Transformation Rules
## In-Flight Transformation Rules

All transformations are applied by the generic `bulk_load.py` PySpark job during the GCS-to-BigQuery load phase. No pre-conversion step — Spark reads raw HDFS-format files from GCS and transforms in memory before writing via the Storage Write API.

### Rule 1: Storage Format Handling

The Spark job selects a reader based on the `source.format` field in each table's YAML manifest:

| Format | Tables | Spark Reader | Notes |
|---|---|---|---|
| **TEXTFILE (CSV)** | `sales_retail`, `returns_cdc`, `return_authorizations`, `shipment_tracking`, `delivery_routes`, `customer_complaints`, `chat_transcripts` | `spark.read.csv()` with `header=True`, `nullValue=''` | Matches Hive `skip.header.line.count=1` and `serialization.null.format=''` |
| **TEXTFILE (TSV)** | `omniture_logs` | `spark.read.csv(sep='\t')` | 61 columns, no header |
| **Parquet** | `pos_transactions`, `inventory_movements`, `warehouse_picks` + all retail managed tables + all staging tables | `spark.read.parquet()` | Native, no conversion needed |
| **Avro** | `customer_signups`, `fraud_signals` | `spark.read.format('avro')` | Requires `spark-avro` package on Dataproc |
| **RCFile** | `product_catalog_feed` | `spark.read.format('hive')` via HiveContext with Hive SerDe JARs | Spark reads RCFile via Hive compatibility layer. MAP column extracted as MapType then serialized to JSON string. |
| **SequenceFile** | `supplier_invoices` | Custom Hadoop InputFormat reader: `sc.sequenceFile()` then parse value bytes | SequenceFile key=invoice_no, value=serialized row. Parse via Hive SequenceFile InputFormat. |
| **JSON SerDe** | `email_campaign_clicks`, `driver_logs` | `spark.read.json()` (NDJSON) | Direct JSON parsing; MAP columns become MapType then JSON. |
| **RegexSerDe** | `loyalty_events` | `spark.read.text()` then `regexp_extract()` per column | Applies the same regex pattern from Hive SerDe properties: `^([^\|]+)\|...\|TX:([^;]+);META:(.*)$`. Extracts 7 named groups. |

### Rule 2: Partition Key Conversion

Per the locked Partitioning and Clustering Redesign decision, STRING partition keys (`yyyyMMdd_HH` format) are collapsed to DATE:

| Pattern | Source Columns | Transform | Target Column |
|---|---|---|---|
| **STRING yyyyMMdd_HH** | `date_ts STRING` | `PARSE_DATE('%Y%m%d', SUBSTRING(date_ts, 1, 8))` | `partition_date DATE` |
| **Multi-column INT** | `year INT, month INT, day INT` | `DATE(year, month, day)` | `partition_date DATE` (generated column in DDL) |
| **Multi-column year/month** | `feed_year INT, feed_month INT` | `DATE(feed_year, feed_month, 1)` | `partition_month DATE` (generated column in DDL) |
| **Native DATE** | `sale_date DATE`, `consent_date DATE` | No conversion | Passed through directly |
| **Multi-column with STRING** | `order_year INT, order_month INT, country_partition STRING` | `DATE(order_year, order_month, 1)` | `partition_month DATE` (generated column in DDL) |

**Important**: The original partition columns (`date_ts`, `year`, `month`, `day`, etc.) are always preserved as regular columns in BigQuery for backward compatibility. The `partition_date`/`partition_month` columns are either:
- Populated explicitly by the Spark job (for non-generated columns), or
- Auto-computed by BigQuery generated column expressions (for tables using `AS (DATE(...))` in DDL)

For tables with generated columns in the target DDL (`inventory_movements`, `supplier_invoices`, `mobile_events`, `fact_orders_eu`, etc.), the Spark job writes only the source INT/STRING columns — BigQuery computes the partition column automatically.

### Rule 3: MAP to JSON Conversion

Per the locked Hive MAP and Complex Types decision, all `MAP<STRING,STRING>` columns become `JSON` in BigQuery:

| Table | MAP Column | Spark Transform |
|---|---|---|
| `raw.mobile_events` | `properties` | `to_json(col('properties'))` → writes as JSON string, connector maps to BQ JSON |
| `raw.email_campaign_clicks` | `utm` | `to_json(col('utm'))` |
| `raw.driver_logs` | `extras` | `to_json(col('extras'))` |
| `raw.product_catalog_feed` | `metadata` | `to_json(col('metadata'))` |
| `staging.parsed_loyalty_events` | `meta` | `to_json(col('meta'))` |
| `retail.dim_product_attributes` | `attributes` | `to_json(col('attributes'))` |
| `regional.events_eu` | `event_properties` | `to_json(col('event_properties'))` |
| `regional.fact_mobile_app_events` | `properties` | `to_json(col('properties'))` |

**Null handling**: Hive MAP NULL → BigQuery JSON NULL (preserved). Empty MAP `{}` → JSON `'{}'` (not NULL). Individual null values within a MAP → JSON `{"key": null}`.

### Rule 4: Complex Type Handling

| Type | Transform | Tables Affected |
|---|---|---|
| `ARRAY<STRUCT<...>>` | Direct pass-through — spark-bigquery-connector maps Spark ArrayType(StructType) directly to BQ ARRAY(STRUCT) | `supplier_invoices.line_items`, `mobile_events.items`, `fact_shipments.tracking_events`, `fact_email_engagement.clicks` |
| `STRUCT<...>` | Direct pass-through — StructType maps to BQ STRUCT | `mobile_events.context`, `email_campaign_clicks.geo`, `driver_logs.gps` |

### Rule 5: Type Widening

Hive integer types that don't exist in BigQuery are widened:

| Hive Type | BigQuery Type | Affected Columns |
|---|---|---|
| `TINYINT` | `INT64` | `mobile_events.hour_bucket`, various flag columns |
| `SMALLINT` | `INT64` | Sporadic across staging/retail |
| `INT` | `INT64` | All INT columns across 82 tables |
| `BOOLEAN` | `BOOL` | Direct mapping, no transform needed |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | All DECIMAL columns — exact fixed-point, no precision loss |
| `DOUBLE` / `FLOAT` | `FLOAT64` | All floating-point columns |

### Rule 6: ACID Table Handling

The 5 Hive ACID tables (`returns_ledger`, `acid_customer_address_history`, `acid_supplier_terms_history`, `acid_loyalty_points_ledger`, `acid_inventory_adjustments_log`) require special extraction:

1. **Read via Hive-on-Spark** or use ORC reader with ACID support — Spark must read the compacted base + delta files, not raw ORC files
2. The Spark job reads the materialized view of each ACID table (post-compaction state) from GCS
3. **Pre-migration step**: Run `ALTER TABLE ... COMPACT 'major'` on each ACID table on the source cluster before DistCp to ensure deltas are merged into base files
4. Spark then reads the base ORC files via `spark.read.orc()` — no delta resolution needed

### Rule 7: RegexSerDe Special Handling (loyalty_events)

```python
# In bulk_load.py — regex_serde reader handler
raw_df = spark.read.text(gcs_path)
parsed_df = raw_df.select(
    regexp_extract('value', pattern, 1).alias('event_ts_str'),
    regexp_extract('value', pattern, 2).alias('member_id'),
    regexp_extract('value', pattern, 3).alias('event_type'),
    regexp_extract('value', pattern, 4).alias('points'),
    regexp_extract('value', pattern, 5).alias('store_id'),
    regexp_extract('value', pattern, 6).alias('tx_id'),
    regexp_extract('value', pattern, 7).alias('meta_raw'),
)
# meta_raw stays as STRING — the parse_key_value_pairs JS UDF handles
# conversion at query time, matching the Hive pattern where str_to_map()
# was applied in downstream queries, not at table level.
```

### Transformation Summary by Database

| Database | Tables | Key Transforms |
|---|---|---|
| **raw** | 17 | STRING partition→DATE (12 tables), MAP→JSON (4 tables), RegexSerDe parsing (1), RCFile reading (1), SequenceFile reading (1), JSON SerDe reading (2), type widening (all) |
| **staging** | 10 | STRING partition→DATE (5 tables), MAP→JSON (1 table), type widening (all) |
| **retail** | 42 | Bucketing dropped (8 tables), ACID compaction read (5 tables), MAP→JSON (1 table), ARRAY/STRUCT pass-through (3 tables), type widening (all) |
| **regional** | 13 | Multi-col partition→DATE (2 tables), MAP→JSON (2 tables), bucketing dropped (1 table), type widening (all) |

## Performance & Throughput
## Performance and Throughput Strategy

### Tiered Parallelism Model

Tables are grouped into 3 waves (US) + 1 wave (EU) based on estimated data volume, with different concurrency limits per wave:

| Wave | Tables | Concurrency | Dataproc Config | Rationale |
|---|---|---|---|---|
| **Wave 1 — Small** | ~35 tables: all `dim_*`, `bridge_*`, `agg_*`, `top_countries_daily`, `sales_cube`, small reference tables | 10-15 concurrent Dataproc Serverless jobs | 2 workers, `n2-standard-4` equivalent | Small tables (< 1 GB) — dominated by job startup overhead. High concurrency maximizes throughput. |
| **Wave 2 — Medium** | ~30 tables: all `staging.*`, medium fact tables (`fact_web_session`, `fact_returns`, `fact_warehouse_picks`, `fact_chat_interactions`, etc.) | 5-8 concurrent | 4 workers, `n2-standard-8` equivalent | Mid-range tables (1-50 GB). Moderate concurrency balances throughput vs. Dataproc quota. |
| **Wave 3 — Large** | ~17 tables: `fact_sales`, `omniture_logs`, `pos_transactions`, `mobile_events`, `fact_inventory_movements`, `fact_payments`, `fact_shipments`, 5 ACID tables | 3-4 concurrent | 8-16 workers, `n2-standard-16` equivalent, autoscaling enabled | Large tables (50+ GB). Lower concurrency prevents Storage Write API throttling and GCS read contention. |
| **EU Wave** | 13 tables: all `regional.*` | 4-5 concurrent | 4 workers, `n2-standard-8` equivalent | Separate Dataproc in `europe-west1`. Runs in parallel with US waves. |

### Wave Dependencies

```
distcp_phase (all 3 clusters in parallel)
    |
    +---> load_wave_1 -----> load_wave_2 -----> load_wave_3
    |                                                |
    +---> load_eu_tables (parallel with US waves)    |
                |                                    |
                +---> inline_validate_eu             +---> record_watermark
                                                          |
                                                          +---> notify_complete
```

Waves 1/2/3 are sequential (not parallel) to control total Dataproc resource consumption and Storage Write API load. The EU wave runs independently and in parallel with US waves since it uses a different project, region, and Dataproc cluster.

### DistCp Throughput

| Cluster | Estimated Data | DistCp Config | Target Duration |
|---|---|---|---|
| acme-lake (raw + staging) | ~500 GB - 2 TB | 20 map tasks, bandwidth limit 500 MB/s | 1-4 hours |
| acme-analytics (retail) | ~1-5 TB | 30 map tasks, bandwidth limit 500 MB/s | 2-8 hours |
| acme-edge (regional) | ~100-500 GB | 10 map tasks, bandwidth limit 200 MB/s | 1-2 hours |

All 3 DistCp jobs run in parallel. Total DistCp phase: bounded by the largest cluster (acme-analytics), estimated 2-8 hours depending on data volume.

### BigQuery Storage Write API Considerations

| Constraint | Limit | Mitigation |
|---|---|---|
| Concurrent streams per project | 10,000 | Well within limits — each Spark executor opens ~1 stream per partition write |
| Throughput per project | 3 GB/s default | Wave 3 concurrency of 3-4 keeps aggregate write rate within quota. Request quota increase if needed. |
| Append quota per table | Unlimited for committed mode | Using `writeMethod=direct` (committed mode) — no streaming buffer limitations |
| Row size limit | 10 MB per row | No tables approach this — largest row is `omniture_logs` at ~61 STRING columns |

### Spark Job Configuration

```python
# Common spark-bigquery-connector settings for all tables
spark.conf.set("spark.datasource.bigquery.writeMethod", "direct")
spark.conf.set("spark.datasource.bigquery.temporaryGcsBucket", staging_bucket)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# For large tables (Wave 3) — increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.datasource.bigquery.writeAtLeastOneRecord", "false")
```

### Estimated Total Pipeline Duration

| Phase | Estimated Duration |
|---|---|
| DistCp (all 3 clusters parallel) | 2-8 hours |
| Wave 1 — small tables (35 tables, 10-15 concurrent) | 30-60 minutes |
| Wave 2 — medium tables (30 tables, 5-8 concurrent) | 1-3 hours |
| Wave 3 — large tables (17 tables, 3-4 concurrent) | 3-8 hours |
| EU wave (13 tables, parallel with US) | 1-2 hours |
| Inline validation | 30-60 minutes |
| **Total estimated** | **8-20 hours** |

### Cost Controls

- **Dataproc Serverless**: Pay per vCPU-hour, no idle cluster costs. Jobs auto-terminate on completion.
- **GCS staging buckets**: Set lifecycle rule to auto-delete objects after 30 days (migration staging data is ephemeral).
- **Storage Write API**: No additional cost beyond standard BigQuery storage ingestion pricing.
- **Composer**: Uses existing Composer environment (provisioned for Oozie migration per locked decision). No incremental cost for the bulk load DAG.

## Validation Strategy
## Inline Validation Strategy (Bulk Load Pipeline)

This decision covers the validation performed **within the bulk load pipeline itself** — the fast smoke tests that run immediately after each table loads. The full 4-layer validation (column aggregates, row-level fingerprints, BAQs) is a separate downstream pipeline defined by the locked Data Validation and Reconciliation decision.

### Inline Validation Scope: Row Count + Null-Key Check

After each Spark load task completes, a `BigQueryCheckOperator` task runs two checks:

**Check 1 — Row Count Parity**
```sql
-- Executed as a Composer BigQueryCheckOperator
SELECT
  CASE WHEN COUNT(*) = @expected_row_count THEN TRUE
       ELSE ERROR(FORMAT('Row count mismatch for %s.%s: expected %d, got %d',
                         @dataset, @table, @expected_row_count, COUNT(*)))
  END
FROM `@project.@dataset.@table`
```

The `expected_row_count` is captured pre-migration from Hive via:
```sql
-- Run on source cluster before DistCp, output stored in manifest
SET hive.local.time.zone=UTC;
SELECT COUNT(*) FROM ${db}.${table}
WHERE ${watermark_col} <= TIMESTAMP '${W}';
```

For partitioned tables, the check also validates partition count:
```sql
SELECT
  CASE WHEN COUNT(DISTINCT partition_date) = @expected_partition_count THEN TRUE
       ELSE ERROR(FORMAT('Partition count mismatch for %s.%s: expected %d, got %d',
                         @dataset, @table, @expected_partition_count,
                         COUNT(DISTINCT partition_date)))
  END
FROM `@project.@dataset.@table`
```

**Check 2 — Null Key Validation**
```sql
-- Validates that natural/primary key columns have no unexpected NULLs
SELECT
  CASE WHEN COUNTIF(@key_col IS NULL) = @expected_null_count THEN TRUE
       ELSE ERROR(FORMAT('Unexpected NULLs in %s.%s.%s: expected %d, got %d',
                         @dataset, @table, @key_col,
                         @expected_null_count, COUNTIF(@key_col IS NULL)))
  END
FROM `@project.@dataset.@table`
```

Key columns per table are defined in the YAML manifest under `validation.null_check_cols`. Examples:
- `fact_sales`: `[invoice_no, customer_sk, product_sk]`
- `returns_ledger`: `[return_id, invoice_no]`
- `dim_gdpr_consent`: `[consent_id, customer_id, granted]` (BOOLEAN — must not be NULL per GDPR)
- `pos_transactions`: `[txn_id, invoice_no]`

### Pre-Migration Row Count Capture

Before the DistCp phase begins, a preparatory step captures source row counts from all 82 Hive tables:

1. A Composer task SSHs to each cluster (or submits Beeline queries via `BashOperator`)
2. Runs `SELECT COUNT(*) FROM ${db}.${table} WHERE ${watermark_col} <= '${W}'` for each table
3. Results are written to a JSON manifest: `gs://acme-migration-staging-us/manifests/source_counts.json`
4. The inline validation tasks read expected counts from this manifest

For tables without a watermark column (static dimensions like `dim_date`, `dim_currency_eu`): full table `COUNT(*)` with no time bound.

### Frozen Watermark Recording

Per acceptance criterion 6, the pipeline records the watermark W at completion:

```python
# Final DAG task: record_watermark
record_watermark = BigQueryInsertJobOperator(
    task_id='record_watermark',
    configuration={
        'query': {
            'query': """
                CREATE OR REPLACE TABLE `acme-analytics.raw._migration_metadata` AS
                SELECT
                    CURRENT_TIMESTAMP() AS migration_completed_at,
                    TIMESTAMP '${W}' AS frozen_watermark_w,
                    82 AS total_tables_loaded,
                    'bulk_migration_dag' AS dag_id,
                    '${dag_run_id}' AS dag_run_id
            """,
            'useLegacySql': False,
        }
    },
)
```

The watermark W is agreed upon **before** the pipeline runs (per the locked Data Validation decision, section 3). It is passed as an Airflow variable and used consistently across:
- Source row count capture queries (Hive side)
- DistCp scope (only partitions up to W)
- Inline validation queries (BigQuery side)
- Downstream formal validation pipeline (layers 1-4)

### Failure Handling

| Scenario | Behavior |
|---|---|
| Row count mismatch on a STANDARD table | Task fails, logged, Slack alert. Other tables in the wave continue. Table is flagged for re-run. |
| Row count mismatch on a CRITICAL table | Task fails, logged, Slack alert. Other tables continue. **Blocks formal validation sign-off** until resolved. |
| Null key check fails | Task fails, logged. Likely indicates a data corruption or format parsing issue. Requires investigation before re-run. |
| Spark job fails (OOM, Storage Write API error) | Retried 3x with exponential backoff. If all retries fail, task is marked failed and Slack alert fires. |
| DistCp fails | Retried 2x. If persistent, likely a network or permission issue — requires manual investigation. |

### Handoff to Formal Validation

Once the bulk load pipeline completes:
1. All 82 tables are loaded in BigQuery with data bounded by watermark W
2. `_migration_metadata` table records W and completion timestamp
3. The formal validation pipeline (separate Composer DAG, per locked Data Validation decision) is triggered
4. Formal validation runs layers 1-4: row counts per partition, column aggregates, row-level fingerprints (30 CRITICAL tables), and BAQ-1 through BAQ-5

The inline validation in this pipeline is intentionally lightweight — it catches gross failures fast (empty tables, truncated loads, corrupt parsing) so that the formal validation pipeline doesn't waste hours discovering a table that loaded zero rows.

### Validation Summary

| Check | Scope | Threshold | When |
|---|---|---|---|
| Row count parity | All 82 tables | Exact match (= 0 delta) | After each table load |
| Partition count parity | All partitioned tables | Exact match | After each table load |
| Null key check | All 82 tables (key columns from manifest) | NULL count matches expected | After each table load |
| Watermark recording | Pipeline-level | W recorded and immutable | After all tables load |
| Source count capture | All 82 tables (Hive side) | Successfully captured before DistCp | Before pipeline starts |

## Implementation Approach
## Bulk Historical Data Migration Pipeline — Implementation Approach

### Architecture Overview

A two-phase pipeline migrates all 82 Hive tables across 3 on-prem Cloudera clusters into BigQuery:

**Phase 1 — DistCp to GCS**: Hadoop DistCp copies raw HDFS data from each cluster to GCS staging buckets, preserving the partition directory layout. Runs on existing cluster infrastructure using the GCS connector for Hadoop.

**Phase 2 — Dataproc Spark → BigQuery**: A config-driven PySpark job on Dataproc Serverless reads each table from GCS, applies in-flight transformations, and writes to BigQuery via the spark-bigquery-connector (Storage Write API, `writeMethod=direct`).

### Toolchain Decisions

| Component | Choice | Rationale |
|---|---|---|
| **Extraction engine** | Dataproc Serverless (Spark) | Natively reads all 6 Hive storage formats (TEXTFILE, Parquet, Avro, RCFile, SequenceFile, JSON SerDe). Single toolchain for all 82 tables. |
| **Data staging** | DistCp → GCS | Runs on existing Hadoop clusters, zero new agents, parallel multi-threaded copy, preserves partition directory structure. |
| **BigQuery write method** | Storage Write API via spark-bigquery-connector | Exactly-once semantics, native JSON/ARRAY/STRUCT type support, ~1 GB/s per stream throughput. |
| **Orchestration** | Cloud Composer DAG | Aligns with locked Oozie→Composer decision. Built-in retry/backoff, pool-based parallelism, UI for monitoring 82 table loads. |
| **Job structure** | Config-driven generic PySpark job | Single reusable `bulk_load.py` entry point parameterized by per-table YAML manifests. ~5 format-specific reader handlers for edge cases. |

### GCS Bucket and Dataproc Topology — GDPR Compliant

```
project: acme-analytics (US multi-region)
  gs://acme-migration-staging-us/
    raw/          -- DistCp from acme-lake (17 tables)
    staging/      -- DistCp from acme-lake (10 tables)
    retail/       -- DistCp from acme-analytics (42 tables)
  Dataproc Serverless (us-central1) -- loads 69 US tables

project: acme-analytics-eu (EU multi-region)
  gs://acme-migration-staging-eu/
    regional/     -- DistCp from acme-edge (13 tables)
  Dataproc Serverless (europe-west1) -- loads 13 EU tables
```

EU data never leaves EU regions — DistCp from acme-edge writes directly to the EU bucket, and the EU Dataproc job runs in an EU region. Enforced by organization policy `constraints/gcp.resourceLocations = in:eu-locations` on the EU project.

### Composer DAG Structure

```
bulk_migration_dag
  distcp_phase (TaskGroup)
    distcp_acme_lake         -- raw + staging to gs://acme-migration-staging-us/
    distcp_acme_analytics    -- retail to gs://acme-migration-staging-us/
    distcp_acme_edge         -- regional to gs://acme-migration-staging-eu/
  load_wave_1 (TaskGroup) -- small tables, 10-15 concurrent
    load_dim_date
    load_dim_customer
    ... (~35 dims, bridges, aggs, small facts)
    inline_validate_wave_1
  load_wave_2 (TaskGroup) -- medium tables, 5-8 concurrent
    load_staging_cleansed_orders
    load_fact_web_session
    ... (~30 staging + medium facts)
    inline_validate_wave_2
  load_wave_3 (TaskGroup) -- large tables, 3-4 concurrent
    load_fact_sales
    load_omniture_logs
    load_pos_transactions
    load_mobile_events
    ... (~17 large facts)
    inline_validate_wave_3
  load_eu_tables (TaskGroup) -- EU Dataproc, 4-5 concurrent
    load_fact_orders_eu
    load_dim_gdpr_consent
    ... (13 regional tables)
    inline_validate_eu
  record_watermark        -- Record frozen watermark W timestamp
  notify_complete         -- Slack notification via on_success_callback
```

### Config-Driven Job Design

Each table has a YAML manifest in `config/tables/`. A single `bulk_load.py` PySpark script reads the manifest and dispatches to the appropriate reader/transform chain.

**Example — standard table with partition key conversion:**
```yaml
# config/tables/raw/sales_retail.yaml
source:
  database: raw
  table: sales_retail
  cluster: acme-lake
  format: textfile
  gcs_path: gs://acme-migration-staging-us/raw/sales/
  partition_cols: [date_ts]

target:
  project: acme-analytics
  dataset: raw
  table: sales_retail

transforms:
  partition_key_conversion:
    source_col: date_ts
    target_col: partition_date
    parse_format: "%Y%m%d_%H"
    parse_fn: PARSE_DATE
  type_widening:
    - {col: quantity, from: INT, to: INT64}

validation:
  watermark_col: null
  expected_row_count: 2847293
  null_check_cols: [invoice_no]
```

**Example — RegexSerDe edge case:**
```yaml
# config/tables/raw/loyalty_events.yaml
source:
  database: raw
  table: loyalty_events
  cluster: acme-lake
  format: regex_serde
  gcs_path: gs://acme-migration-staging-us/raw/loyalty_events/
  partition_cols: [date_ts]
  regex_pattern: '^([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|TX:([^;]+);META:(.*)$'
  regex_columns: [event_ts_str, member_id, event_type, points, store_id, tx_id, meta_raw]

target:
  project: acme-analytics
  dataset: raw
  table: loyalty_events

transforms:
  partition_key_conversion:
    source_col: date_ts
    target_col: partition_date
    parse_format: "%Y%m%d_%H"
    parse_fn: PARSE_DATE
```

### Retry and Error Handling

| Scope | Setting | Value |
|---|---|---|
| Spark load tasks | retries | 3 |
| Spark load tasks | retry_delay | 5 minutes, exponential backoff |
| DistCp tasks | retries | 2, 10 minute delay |
| DAG-level | max_active_runs | 1 (prevents concurrent bulk loads) |
| On failure | notification | SlackWebhookOperator via on_failure_callback |
| Table isolation | blast radius | Single table failure does NOT block other tables in same wave |
