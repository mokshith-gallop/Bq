# Implementation Approach

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
