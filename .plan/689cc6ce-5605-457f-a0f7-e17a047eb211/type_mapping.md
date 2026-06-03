# Type Mapping

## Complete Hive → BigQuery Type Mapping

### Scalar Type Mapping

| Hive Type | BigQuery Type | Notes |
|---|---|---|
| `STRING` | `STRING` | Direct mapping |
| `INT` | `INT64` | BQ has no 32-bit integer type |
| `BIGINT` | `INT64` | Direct mapping |
| `TINYINT` | `INT64` | Widened — BQ has no narrow int types |
| `SMALLINT` | `INT64` | Widened — BQ has no narrow int types |
| `BOOLEAN` | `BOOL` | Direct mapping |
| `FLOAT` | `FLOAT64` | Widened from 32-bit to 64-bit; validation uses ±0.001% tolerance |
| `DOUBLE` | `FLOAT64` | Direct mapping |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | Preserves exact precision and scale. Covers all source precisions up to (14,2). BQ NUMERIC supports up to (38,9). |
| `TIMESTAMP` | `TIMESTAMP` | All zone-naive Hive timestamps interpreted as UTC per locked validation decision |
| `DATE` | `DATE` | Direct mapping |

### Complex Type Mapping (per locked MAP/Complex Types decision)

| Hive Type | BigQuery Type | Tables Affected |
|---|---|---|
| `MAP<STRING,STRING>` | `JSON` | `raw.mobile_events.properties`, `raw.loyalty_events.meta_raw` (STRING→JSON via UDF), `raw.customer_complaints.resolution_details`, `raw.chat_transcripts.session_metadata`, `raw.email_campaign_clicks.utm`, `raw.driver_logs.extras`, `staging.parsed_loyalty_events.meta_map`, `retail.fact_app_clicks.properties`, `retail.dim_product_attributes.attributes`, `regional.events_eu.payload_json` (STRING→JSON), `regional.fact_mobile_app_events.properties` |
| `ARRAY<STRUCT<...>>` | `ARRAY<STRUCT<...>>` | `raw.supplier_invoices.line_items`, `raw.mobile_events.items`, `retail.fact_shipments.tracking_events`, `retail.fact_email_engagement.clicks` |
| `STRUCT<...>` | `STRUCT<...>` | `raw.mobile_events.context`, `raw.email_campaign_clicks.geo`, `raw.driver_logs.gps`, `retail.fact_app_clicks.device`, `regional.fact_mobile_app_events.device` |
| `ARRAY<STRING>` | `ARRAY<STRING>` | `regional.dim_product_eu_catalog.eu_compliance_flags`, `staging.fraud_scored.signals` |

### Special Column Notes

**`raw.loyalty_events.meta_raw`**: This is a `STRING` column (not MAP) that gets parsed via `str_to_map()` at query time. In BigQuery, this column stays as `STRING` in the table DDL — the `parse_key_value_pairs` JS UDF (from locked UDF decision) handles the conversion in views/queries. It does NOT become JSON at the table level.

**`regional.events_eu.payload_json`**: This is a `STRING` column that holds JSON data. In BigQuery it stays as `STRING` in the DDL (the column name already indicates JSON content). Downstream queries can use `JSON_VALUE()` / `JSON_QUERY()` with a `SAFE.PARSE_JSON()` wrapper if needed.

### Partition Column Type Conversions
Per the locked partitioning decision, partition columns undergo these transformations:

| Pattern | Source Columns | BigQuery DDL Pattern |
|---|---|---|
| STRING date_ts `yyyyMMdd_HH` → DATE | `raw.sales_retail.date_ts`, `raw.loyalty_events.date_ts`, `raw.email_campaign_clicks.date_ts`, `raw.driver_logs.date_ts`, etc. (12 raw tables) | Original STRING preserved as regular column. New `partition_date DATE` column added. `PARTITION BY partition_date`. |
| Multi-col `year/month/day` → DATE | `raw.inventory_movements`, `raw.supplier_invoices`, `retail.fact_inventory_movements`, `retail.fact_shipments` | Original INT columns preserved. `PARTITION BY DATE(year, month, day)` via generated column or `PARSE_DATE`. |
| Multi-col `year/month` → DATE | `retail.fact_payments`, `regional.fact_orders_eu` | `PARTITION BY DATE(post_year, post_month, 1)` — day defaults to 1st. |
| Multi-col STRING date + secondary → DATE + CLUSTER | `raw.mobile_events`, `raw.shipment_tracking`, `raw.warehouse_picks`, `staging.dedup_clickstream`, `retail.fact_web_session`, `retail.fact_app_clicks`, `regional.fact_mobile_app_events` | Date column → `PARTITION BY`. Secondary column → `CLUSTER BY`. |
| DATE partition (native) | `retail.fact_sales.sale_date`, `regional.fact_returns_eu.return_date`, `regional.dim_gdpr_consent.consent_date`, etc. | `PARTITION BY sale_date` — direct, no conversion. |
| Bucketing only (no partition) | `retail.returns_ledger` (CLUSTERED BY return_id INTO 4), ACID tables | `CLUSTER BY bucket_column`. No PARTITION BY unless a date column is suitable. |

### Extended Partition Conversion Table
The locked decision covers 9 tables explicitly. Here are the remaining partitioned tables applying the same rules:

| Table | Hive Partition | BQ Partition | BQ Cluster By |
|---|---|---|---|
| `raw.omniture_logs` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.loyalty_events` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.email_campaign_clicks` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.shipment_tracking` | `date_ts STRING, carrier_partition STRING` | `PARTITION BY partition_date` | `CLUSTER BY carrier_partition` |
| `raw.warehouse_picks` | `date_ts STRING, warehouse_id_partition STRING` | `PARTITION BY partition_date` | `CLUSTER BY warehouse_id_partition` |
| `raw.driver_logs` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.customer_complaints` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.chat_transcripts` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.returns_cdc` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.pos_transactions` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.return_authorizations` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.delivery_routes` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.product_catalog_feed` | `date_ts STRING` | `PARTITION BY partition_date` | — |
| `raw.supplier_invoices` | `feed_year INT, feed_month INT` | `PARTITION BY DATE(feed_year, feed_month, 1)` | — |
| `staging.dedup_clickstream` | `date_ts STRING, country_partition STRING` + BUCKETED BY user_id INTO 16 | `PARTITION BY partition_date` | `CLUSTER BY country_partition, user_id` |
| `staging.cleansed_orders` | `order_date DATE` | `PARTITION BY order_date` | — |
| `retail.fact_returns` | `return_date DATE` | `PARTITION BY return_date` | — |
| `retail.fact_app_clicks` | `event_date DATE, platform_partition STRING` | `PARTITION BY event_date` | `CLUSTER BY platform_partition` |
| `retail.returns_ledger` | (none — ACID, bucketed by return_id INTO 4) | No partition | `CLUSTER BY return_id` |
| `retail.acid_customer_address_history` | (none — ACID, bucketed by customer_sk INTO 8) | No partition | `CLUSTER BY customer_sk` |
| `retail.acid_supplier_terms_history` | (none — ACID, bucketed by supplier_sk INTO 4) | No partition | `CLUSTER BY supplier_sk` |
| `retail.acid_loyalty_points_ledger` | (none — ACID, bucketed by member_id INTO 8) | No partition | `CLUSTER BY member_id` |
| `retail.acid_inventory_adjustments_log` | (none — ACID, bucketed by adjustment_id INTO 4) | No partition | `CLUSTER BY adjustment_id` |
| `regional.fact_returns_eu` | `return_date DATE` | `PARTITION BY return_date` | — |
| `regional.fact_shipments_eu` | `ship_date DATE` | `PARTITION BY ship_date` | — |
| `regional.dim_gdpr_consent` | `consent_date DATE` | `PARTITION BY consent_date` | — |
| `regional.staging_orders_eu` | `snapshot_date DATE` | `PARTITION BY snapshot_date` | — |
| `regional.staging_customers_eu_cdc` | `snapshot_date DATE` | `PARTITION BY snapshot_date` | — |
| `regional.fact_eu_promotions` | `redemption_date DATE` | `PARTITION BY redemption_date` | — |
| `regional.events_eu` | `event_date STRING` | `PARTITION BY partition_date` | — |
| `regional.fact_mobile_app_events` | `event_date STRING, platform_partition STRING` | `PARTITION BY PARSE_DATE('%Y%m%d', event_date)` | `CLUSTER BY platform_partition` |

### Non-Partitioned Tables (no conversion needed)
These tables have no partitions or bucketing in Hive. They become plain BigQuery managed tables:
- `retail.dim_date`, `retail.dim_customer`, `retail.dim_product`, `retail.dim_employee_history`, `retail.dim_store_history`
- `retail.sales_cube`, `retail.top_countries_daily`
- All `retail.agg_*` tables (7 tables)
- All `retail.bridge_*` tables (5 tables)
- `retail.fact_web_session` (already in locked table — partitioned)
- `regional.dim_currency_eu`, `regional.dim_product_eu_catalog`, `regional.dim_locale_eu`, `regional.dim_customer_snapshot`

### Kudu Snapshot Table Type Conversions
| Kudu Column | Kudu Type | BQ Snapshot Type | Notes |
|---|---|---|---|
| `last_updated_ts` | `BIGINT` (epoch ms) | `TIMESTAMP` | Convert via `TIMESTAMP_MILLIS()` during load |
| `started_ts` | `BIGINT` (epoch ms) | `TIMESTAMP` | Convert via `TIMESTAMP_MILLIS()` during load |
| `last_event_ts` | `BIGINT` (epoch ms) | `TIMESTAMP` | Convert via `TIMESTAMP_MILLIS()` during load |
| `valid_from_ts` | `BIGINT` (epoch ms) | `TIMESTAMP` | Convert via `TIMESTAMP_MILLIS()` during load |
| `valid_to_ts` | `BIGINT` (epoch ms) | `TIMESTAMP` | Convert via `TIMESTAMP_MILLIS()` during load |
| `updated_ts` | `BIGINT` (epoch ms) | `TIMESTAMP` | Convert via `TIMESTAMP_MILLIS()` during load |
| `DECIMAL(12,2)` | Kudu DECIMAL | `NUMERIC(12,2)` | Same as standard mapping |
| `DECIMAL(10,2)` | Kudu DECIMAL | `NUMERIC(10,2)` | Same as standard mapping |
| `DECIMAL(5,4)` | Kudu DECIMAL | `NUMERIC(5,4)` | Same as standard mapping |
