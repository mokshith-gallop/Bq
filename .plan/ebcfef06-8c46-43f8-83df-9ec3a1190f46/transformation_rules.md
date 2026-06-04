# Transformation Rules

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
