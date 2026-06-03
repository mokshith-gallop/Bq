# Locked Decisions for Story 689cc6ce-5605-457f-a0f7-e17a047eb211

## Type Mapping
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

## Validation Strategy
## Validation Strategy: DDL Correctness Verification

This story produces BigQuery DDL files, not data. Validation focuses on verifying the generated DDL is correct, complete, and aligned with all locked decisions. Data validation happens in a separate story using the locked Data Validation & Reconciliation Methodology.

### 1. Coverage Check — All Objects Accounted For

**Tables (86 total = 82 source + 4 Kudu snapshots)**:
Every table in the source schema must have a corresponding `.sql` file under `ddl/{dataset}/tables/`. Verify:

| Dataset | Expected Count | Source |
|---|---|---|
| `raw/tables/` | 17 files | `db_raw` — 17 tables |
| `staging/tables/` | 10 files | `db_staging` — 10 tables |
| `retail/tables/` | 46 files | `db_retail` — 42 tables + 4 Kudu snapshot tables (`inventory_realtime_snapshot`, `session_state_snapshot`, `promo_eligibility_snapshot`, `realtime_price_snapshot`) |
| `regional_eu/tables/` | 13 files | `db_regional` — 13 tables |
| **Total** | **86 files** | |

**Views (15 total)**:
| Dataset | Expected Views | Count |
|---|---|---|
| `raw/views/` | `omniture.sql`, `v_fraud_signals_recent.sql` | 2 |
| `staging/views/` | `v_returns_pending.sql` | 1 |
| `retail/views/` | `vw_daily_sales_by_country.sql`, `vw_weekly_sales_with_running_totals.sql`, `vw_customer_lifetime_value.sql`, `vw_product_performance.sql`, `vw_monthly_cohort_retention.sql`, `vw_session_to_order_attribution.sql`, `vw_active_member_panel.sql`, `vw_sales_rollup_by_region.sql`, `vw_category_hierarchy_recursive.sql`, `vw_panel_continuity_score.sql`, `vw_otd_by_carrier_30d.sql` | 11 |
| `regional_eu/views/` | `v_eu_orders_with_consent.sql` | 1 |
| **Total** | | **15** |

**Supporting files**: `deploy_order.txt`, `variables.env`

**Grand total**: 86 tables + 15 views + 2 supporting files = **103 files**

### 2. Structural Checks Per DDL File

For each generated table DDL, verify:

| Check | Rule | Fail Condition |
|---|---|---|
| **Statement type** | Tables use `CREATE TABLE IF NOT EXISTS`. Views use `CREATE OR REPLACE VIEW`. | Wrong statement type |
| **Fully-qualified name** | Every object references `${PROJECT_US}.${DS_*}.table_name` or `${PROJECT_EU}.${DS_REGIONAL}.table_name` | Missing or hardcoded project ID |
| **Column count** | BQ column count ≥ Hive column count (may be higher due to added `partition_date` columns) | Fewer columns than source |
| **No Hive-isms** | No `STORED AS`, `TBLPROPERTIES`, `ROW FORMAT`, `LOCATION`, `CLUSTERED BY ... INTO N BUCKETS`, `EXTERNAL` | Any Hive-only syntax present |
| **OPTIONS block** | Every table has `description` and `labels` in OPTIONS | Missing OPTIONS |
| **Labels** | `source_system=cloudera`. `migration_tier` is `critical` for 30 CRITICAL tables, `standard` for 52 STANDARD tables. | Wrong tier label |
| **Semicolon terminated** | Every statement ends with `;` | Missing terminator |

### 3. Type Mapping Compliance

For each column in every table DDL, verify against the locked Type Mapping decision:

| Source Type | Expected BQ Type | Spot-Check Tables |
|---|---|---|
| `INT` / `TINYINT` / `SMALLINT` | `INT64` | `raw.mobile_events.hour_bucket` (TINYINT→INT64), `retail.fact_sales.quantity` (INT→INT64) |
| `BIGINT` | `INT64` | `retail.returns_ledger.return_id`, `retail.acid_loyalty_points_ledger.entry_id` |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` with matching p,s | `retail.fact_sales.unit_price` → `NUMERIC(10,2)`, `retail.fact_sales.line_total` → `NUMERIC(14,2)`, `regional.fact_orders_eu.vat_amount` → `NUMERIC(12,2)`, `staging.fraud_scored.fraud_score` → `NUMERIC(5,4)` |
| `FLOAT` / `DOUBLE` | `FLOAT64` | `staging.geocoded_addresses.lat`, `staging.geocoded_addresses.lon` |
| `BOOLEAN` | `BOOL` | `regional.dim_gdpr_consent.granted`, `retail.acid_customer_address_history.is_current` |
| `MAP<STRING,STRING>` | `JSON` | `raw.mobile_events.properties`, `raw.email_campaign_clicks.utm`, `retail.fact_app_clicks.properties`, `regional.fact_mobile_app_events.properties` |
| `ARRAY<STRUCT<...>>` | `ARRAY<STRUCT<...>>` | `raw.supplier_invoices.line_items`, `retail.fact_shipments.tracking_events` |
| `STRUCT<...>` | `STRUCT<...>` | `raw.mobile_events.context`, `regional.fact_mobile_app_events.device` |
| Kudu `BIGINT` epoch-ms | `TIMESTAMP` | All 4 snapshot tables: `last_updated_ts`, `started_ts`, etc. |

### 4. Partitioning & Clustering Compliance

Verify every partitioned table's DDL matches the locked Partitioning & Clustering decision:

**Explicit conversion table entries (9 tables)** — must match exactly:
- `raw.sales_retail`: `PARTITION BY partition_date` + `OPTIONS(require_partition_filter=TRUE)`
- `raw.mobile_events`: `PARTITION BY PARSE_DATE(...)` + `CLUSTER BY hour_bucket`
- `raw.inventory_movements`: `PARTITION BY DATE(year, month, day)` + `CLUSTER BY region`
- `retail.fact_web_session`: `PARTITION BY event_date` + `CLUSTER BY country`
- `retail.fact_inventory_movements`: `PARTITION BY DATE(year, month, day)` + `CLUSTER BY region`
- `retail.fact_payments`: `PARTITION BY DATE(post_year, post_month, 1)` + `CLUSTER BY payment_method_partition`
- `retail.fact_shipments`: `PARTITION BY DATE(ship_year, ship_month, ship_day)` + `CLUSTER BY carrier_partition`
- `retail.fact_sales`: `PARTITION BY sale_date` + `CLUSTER BY customer_sk`
- `regional.fact_orders_eu`: `PARTITION BY DATE(order_year, order_month, 1)` + `CLUSTER BY country_partition, customer_id`

**Rule-derived entries** — verify pattern compliance:
- All `date_ts STRING` partition tables → have `partition_date DATE` column + `PARTITION BY partition_date`
- All multi-column partition tables → single DATE partition + secondary columns in CLUSTER BY
- All bucketed tables → `CLUSTER BY` on bucket column(s), no `INTO N BUCKETS`
- No table has more than 4 CLUSTER BY columns (BigQuery limit)

**ACID tables (5)** — verify:
- No `PARTITION BY` (these were non-partitioned in Hive)
- `CLUSTER BY` on the original bucket column
- No transactional properties

**require_partition_filter** — verify `OPTIONS(require_partition_filter=TRUE)` present on:
`raw.sales_retail`, `raw.omniture_logs`, `raw.pos_transactions`, `raw.mobile_events`, `retail.fact_sales`, `retail.fact_web_session`, `retail.fact_payments`, `retail.fact_shipments`, `retail.fact_inventory_movements`, `regional_eu.fact_orders_eu`, `regional_eu.fact_mobile_app_events`

### 5. View Dialect Translation Compliance

For each view, verify all Hive/Impala constructs are translated:

| View | Dialect Constructs to Verify |
|---|---|
| `vw_customer_lifetime_value` | `DATEDIFF(a,b)` → `DATE_DIFF(a,b,DAY)`, `date_sub(current_date(),N)` → `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)` |
| `vw_monthly_cohort_retention` | `DATE_FORMAT(d,'yyyy-MM')` → `FORMAT_DATE('%Y-%m',d)`, `MONTHS_BETWEEN` → `DATE_DIFF(...,MONTH)`, `to_date(concat(...))` → `PARSE_DATE(...)` |
| `vw_active_member_panel` | `NDV(col)` → `APPROX_COUNT_DISTINCT(col)`, `date_sub` → `DATE_SUB` with INTERVAL |
| `vw_sales_rollup_by_region` | `GROUPING__ID` → `GROUPING_ID(s.region, s.store_sk)`, `GROUP BY ... WITH ROLLUP` → `GROUP BY ROLLUP(...)` |
| `vw_panel_continuity_score` | `normalize_country(x)` → fully-qualified `` `${PROJECT_US}.${DS_UDFS}.normalize_country`(x) `` |
| `vw_otd_by_carrier_30d` | `unix_timestamp(ts)` → `UNIX_SECONDS(ts)` or `TIMESTAMP_DIFF`, `INTERVAL '48' HOUR` → `INTERVAL 48 HOUR` |
| `vw_session_to_order_attribution` | Cross-dataset: `raw.mobile_events` → `` `${PROJECT_US}.${DS_RAW}.mobile_events` ``, STRUCT access `s.context.referrer` preserved |
| `v_fraud_signals_recent` | `date_format(date_sub(...), 'yyyyMMdd')` → `FORMAT_DATE('%Y%m%d', DATE_SUB(..., INTERVAL 1 DAY))` |
| `v_returns_pending` | `DATEDIFF(current_date(), to_date(r.requested_at))` → `DATE_DIFF(CURRENT_DATE(), DATE(r.requested_at), DAY)`, cross-dataset ref |
| `v_eu_orders_with_consent` | All refs → `` `${PROJECT_EU}.${DS_REGIONAL}.table` ``, no dialect issues (clean SQL) |

**No Hive dialect residue**: Grep all view files for: `DATEDIFF(`, `DATE_FORMAT(`, `MONTHS_BETWEEN(`, `NDV(`, `GROUPING__ID`, `WITH ROLLUP`, `ILIKE`, `DECODE(`, `unix_timestamp(`, `date_sub(current_date(),` (without INTERVAL). Any match = FAIL.

### 6. Cross-Reference Integrity

| Check | Method |
|---|---|
| **deploy_order.txt completeness** | Every `.sql` file under `ddl/` appears exactly once in `deploy_order.txt` |
| **deploy_order.txt ordering** | All tables before all views. Within views, dependencies come before dependents. `vw_panel_continuity_score` is last among retail views (UDF dependency). |
| **variables.env completeness** | All 7 template variables (`PROJECT_US`, `PROJECT_EU`, `DS_RAW`, `DS_STAGING`, `DS_RETAIL`, `DS_REGIONAL`, `DS_UDFS`) are defined with defaults |
| **No undefined variables** | Grep all `.sql` files for `${...}` patterns — every match must be one of the 7 defined variables |
| **Regional dataset routing** | All `regional.*` tables/views reference `${PROJECT_EU}.${DS_REGIONAL}`, never `${PROJECT_US}` |
| **US dataset routing** | All `raw.*`, `staging.*`, `retail.*` tables/views reference `${PROJECT_US}`, never `${PROJECT_EU}` (except cross-project authorized views, if any) |

### 7. Acceptance Criteria Traceability

Each of the 11 acceptance criteria maps to specific validation checks:

| AC # | What It Tests | Validation Checks |
|---|---|---|
| AC-1 | STRING partition → DATE + preserved original | §4: raw tables with `date_ts STRING` have `partition_date DATE` column + original `date_ts STRING` |
| AC-2 | Multi-column → single DATE + CLUSTER BY | §4: 4 named tables match locked conversion table exactly |
| AC-3 | Bucketing → CLUSTER BY, no INTO N BUCKETS | §4: 8 bucketed retail tables have CLUSTER BY, no BUCKETS clause |
| AC-4 | MAP → JSON | §3: 10+ MAP columns typed as JSON |
| AC-5 | ARRAY<STRUCT> preserved | §3: `line_items`, `tracking_events`, `items` remain ARRAY<STRUCT> |
| AC-6 | ACID → standard managed table | §4: 5 ACID tables have no transactional properties, standard BQ tables |
| AC-7 | Kudu snapshot tables exist | §1: 4 snapshot tables present in `retail/tables/` |
| AC-8 | 3 raw/staging views + 1 regional view translated | §5: dialect translation verified for `omniture`, `v_fraud_signals_recent`, `v_returns_pending`, `v_eu_orders_with_consent` |
| AC-9 | 6+ retail analytics views translated | §5: dialect translation verified for all 11 retail views |
| AC-10 | require_partition_filter on raw.sales_retail | §4: OPTIONS check confirms flag is present |
| AC-11 | **Executability** — all 101 DDL statements compile and execute with zero errors | §8: BigQuery dry-run or apply-and-drop in scratch dataset confirms zero failures |

### 8. Executability Check

**Purpose**: Prove that every generated DDL file is syntactically valid BigQuery SQL and that views resolve all table/column dependencies when executed in `deploy_order.txt` order.

**Procedure**:

1. **Variable substitution**: Run `envsubst` on every `.sql` file using the defaults from `variables.env`:
   - `PROJECT_US=acme-analytics`, `PROJECT_EU=acme-analytics-eu`
   - `DS_RAW=raw`, `DS_STAGING=staging`, `DS_RETAIL=retail`, `DS_REGIONAL=regional_eu`, `DS_UDFS=udfs`

2. **Create scratch datasets** in project `cloudera-env-experiments`:
   ```bash
   bq mk --location=US cloudera-env-experiments:scratch_raw
   bq mk --location=US cloudera-env-experiments:scratch_staging
   bq mk --location=US cloudera-env-experiments:scratch_retail
   bq mk --location=EU cloudera-env-experiments:scratch_regional_eu
   bq mk --location=US cloudera-env-experiments:scratch_udfs
   ```
   After substitution, replace project/dataset references with the scratch equivalents for execution.

3. **Execute in deploy_order.txt sequence**:
   - Process each `.sql` file in the listed order
   - Tables first (all 86), then views (all 15)
   - For each statement, execute against BigQuery (not dry-run — views need real base tables to resolve column references and types)
   - Record: file name, execution status (SUCCESS / FAIL), error message if any, elapsed time

4. **Special handling for UDF-dependent views**:
   - `vw_panel_continuity_score` depends on `udfs.normalize_country` — create a stub JS UDF in `scratch_udfs` before executing this view:
     ```sql
     CREATE OR REPLACE FUNCTION `cloudera-env-experiments.scratch_udfs.normalize_country`(country STRING)
     RETURNS STRING
     LANGUAGE js AS r"""return country;""";
     ```
   - This validates the view's syntax and column resolution without requiring the full UDF implementation

5. **Pass criterion**: **0 failures** across all 101 statements. Any statement that returns a BigQuery error = FAIL for the entire check.

6. **Cleanup**: Drop all scratch datasets after the check completes:
   ```bash
   bq rm -r -f cloudera-env-experiments:scratch_raw
   bq rm -r -f cloudera-env-experiments:scratch_staging
   bq rm -r -f cloudera-env-experiments:scratch_retail
   bq rm -r -f cloudera-env-experiments:scratch_regional_eu
   bq rm -r -f cloudera-env-experiments:scratch_udfs
   ```

7. **Output**: A report listing every file with its execution result. Example:
   ```
   [PASS] raw/tables/sales_retail.sql (0.8s)
   [PASS] raw/tables/mobile_events.sql (0.6s)
   ...
   [PASS] retail/views/vw_panel_continuity_score.sql (1.2s)
   ---
   101/101 PASSED, 0 FAILED
   ```

## Implementation Approach
## Implementation Approach: Hive DDL → BigQuery DDL Generation

### Output Structure
**One file per object**, organized in dataset folders under `/workspace/project/ddl/`:

```
ddl/
├── raw/
│   ├── tables/
│   │   ├── sales_retail.sql
│   │   ├── mobile_events.sql
│   │   ├── omniture_logs.sql
│   │   ├── ... (17 tables total)
│   ├── views/
│   │   ├── omniture.sql
│   │   └── v_fraud_signals_recent.sql
├── staging/
│   ├── tables/
│   │   ├── dedup_clickstream.sql
│   │   ├── ... (10 tables total)
│   ├── views/
│   │   └── v_returns_pending.sql
├── retail/
│   ├── tables/
│   │   ├── fact_sales.sql
│   │   ├── returns_ledger.sql
│   │   ├── ... (42 tables + 4 Kudu snapshot tables = 46 total)
│   ├── views/
│   │   ├── vw_daily_sales_by_country.sql
│   │   ├── vw_weekly_sales_with_running_totals.sql
│   │   ├── vw_customer_lifetime_value.sql
│   │   ├── vw_product_performance.sql
│   │   ├── vw_monthly_cohort_retention.sql
│   │   ├── vw_session_to_order_attribution.sql
│   │   ├── vw_active_member_panel.sql
│   │   ├── vw_sales_rollup_by_region.sql
│   │   ├── vw_category_hierarchy_recursive.sql
│   │   ├── vw_panel_continuity_score.sql
│   │   └── vw_otd_by_carrier_30d.sql
├── regional_eu/
│   ├── tables/
│   │   ├── fact_orders_eu.sql
│   │   ├── dim_gdpr_consent.sql
│   │   ├── ... (13 tables total)
│   ├── views/
│   │   └── v_eu_orders_with_consent.sql
├── deploy_order.txt          -- Ordered list of all files for sequential deployment
└── variables.env             -- Template variable definitions
```

**Total objects**: 82 tables + 4 Kudu snapshot tables + ~14 views = ~100 DDL files.

### Template Variables
All DDL files use substitution variables instead of hardcoded project IDs:

| Variable | Default Value | Description |
|---|---|---|
| `${PROJECT_US}` | `acme-analytics` | US multi-region project |
| `${PROJECT_EU}` | `acme-analytics-eu` | EU multi-region project |
| `${DS_RAW}` | `raw` | Raw landing dataset |
| `${DS_STAGING}` | `staging` | Staging dataset |
| `${DS_RETAIL}` | `retail` | Retail warehouse dataset |
| `${DS_REGIONAL}` | `regional_eu` | EU regional dataset |
| `${DS_UDFS}` | `udfs` | Shared UDF dataset |

A `variables.env` file documents all variables with defaults. A `deploy.sh` script performs `envsubst` on each `.sql` file before execution.

### BigQuery DDL Conventions
Every `CREATE TABLE` statement follows this template:

```sql
CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_sales` (
  invoice_no STRING,
  customer_sk INT64,
  product_sk INT64,
  quantity INT64,
  unit_price NUMERIC(10,2),
  line_total NUMERIC(14,2),
  country STRING,
  invoice_ts TIMESTAMP,
  sale_date DATE
)
PARTITION BY sale_date
CLUSTER BY customer_sk
OPTIONS (
  description = 'Core revenue fact table. Source: retail.fact_sales (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')]
);
```

Key patterns:
- `CREATE TABLE IF NOT EXISTS` — idempotent, safe to re-run
- `CREATE OR REPLACE VIEW` — views are always replaceable
- `OPTIONS(description=...)` includes source lineage (Hive DB, cluster)
- `OPTIONS(labels=...)` includes `source_system=cloudera` and `migration_tier=critical|standard`
- `OPTIONS(require_partition_filter=TRUE)` on high-volume partitioned tables per AC #10

### Deployment Order
The `deploy_order.txt` file lists all DDL files in dependency-safe order:

1. **All tables** (raw → staging → retail → regional_eu) — tables have no cross-dependencies
2. **Kudu snapshot tables** (retail) — after base retail tables
3. **Views in dependency order**:
   - `raw/views/omniture.sql` (depends on `raw.omniture_logs`)
   - `raw/views/v_fraud_signals_recent.sql` (depends on `raw.fraud_signals` — NOTE: this view references a table not in the 82-table inventory; will generate as-is with a comment)
   - `staging/views/v_returns_pending.sql` (depends on `raw.return_authorizations` — cross-dataset)
   - `retail/views/vw_daily_sales_by_country.sql` through `vw_product_performance.sql` (depend on retail tables only)
   - `retail/views/vw_session_to_order_attribution.sql` (cross-dataset: depends on `raw.mobile_events`)
   - `retail/views/vw_panel_continuity_score.sql` (depends on `${DS_UDFS}.normalize_country` UDF — must be deployed AFTER UDFs per locked UDF decision)
   - `regional_eu/views/v_eu_orders_with_consent.sql` (depends on EU tables only)

### ACID Tables → Standard BigQuery Managed Tables
The 5 ACID tables (`returns_ledger`, `acid_customer_address_history`, `acid_supplier_terms_history`, `acid_loyalty_points_ledger`, `acid_inventory_adjustments_log`) become standard BigQuery managed tables:
- Drop all `TBLPROPERTIES('transactional'='true', ...)` — BigQuery natively supports UPDATE/DELETE/MERGE
- Drop `STORED AS ORC` and `orc.compress` — BigQuery manages storage format internally
- Drop `CLUSTERED BY ... INTO N BUCKETS` — converted to `CLUSTER BY` per locked partitioning decision
- Hive bucketing columns become BigQuery CLUSTER BY keys

### Kudu Snapshot Tables
4 new BigQuery tables for batch analytics consumption (per locked Kudu decision):
- `retail.inventory_realtime_snapshot`
- `retail.session_state_snapshot`
- `retail.promo_eligibility_snapshot`
- `retail.realtime_price_snapshot`

Key conversions:
- Kudu `PRIMARY KEY` → not applicable (BigQuery has no primary key enforcement)
- `PARTITION BY HASH` → dropped (no equivalent in BigQuery)
- `BIGINT` epoch-millis columns → `TIMESTAMP` (e.g. `last_updated_ts BIGINT` → `last_updated_ts TIMESTAMP`)
- `STORED AS KUDU` + TBLPROPERTIES → dropped entirely

### View Dialect Translation Rules
Each view's SQL is rewritten following this translation table:

| Hive/Impala | BigQuery | Notes |
|---|---|---|
| `DATEDIFF(a, b)` | `DATE_DIFF(a, b, DAY)` | 2-arg → 3-arg |
| `DATE_FORMAT(d, 'yyyy-MM')` | `FORMAT_DATE('%Y-%m', d)` | Java → POSIX format |
| `DATE_FORMAT(d, 'yyyyMMdd')` | `FORMAT_DATE('%Y%m%d', d)` | |
| `MONTHS_BETWEEN(a, b)` | `DATE_DIFF(a, b, MONTH)` | Returns INT not FLOAT |
| `NDV(col)` | `APPROX_COUNT_DISTINCT(col)` | Impala-specific |
| `GROUPING__ID` | `GROUPING_ID(col1, col2, ...)` | Column → function call with args |
| `GROUP BY a, b WITH ROLLUP` | `GROUP BY ROLLUP(a, b)` | Syntax restructure |
| `date_sub(current_date(), N)` | `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)` | Add INTERVAL keyword |
| `unix_timestamp(ts)` | `UNIX_SECONDS(ts)` | |
| `to_date(concat(...))` | `PARSE_DATE(...)` or `DATE(...)` | Context-dependent |
| `INTERVAL '48' HOUR` | `INTERVAL 48 HOUR` | Remove quotes around number |
| `normalize_country(x)` | `` `${PROJECT_US}.${DS_UDFS}.normalize_country`(x) `` | Fully-qualified UDF ref |
| `raw.table` / `retail.table` | `` `${PROJECT_US}.${DS_RAW}.table` `` | Fully-qualified dataset refs |
| `regional.table` | `` `${PROJECT_EU}.${DS_REGIONAL}.table` `` | EU project for regional |
| `WITH RECURSIVE` | `WITH RECURSIVE` | Direct support in BQ |
| `s.context.referrer` | `s.context.referrer` | STRUCT field access identical |

### require_partition_filter Tables (AC #10)
`OPTIONS(require_partition_filter=TRUE)` is applied to:
- `raw.sales_retail` (explicitly required by AC #10)
- All other high-volume partitioned fact tables: `raw.omniture_logs`, `raw.pos_transactions`, `raw.mobile_events`, `retail.fact_sales`, `retail.fact_web_session`, `retail.fact_payments`, `retail.fact_shipments`, `retail.fact_inventory_movements`, `regional_eu.fact_orders_eu`, `regional_eu.fact_mobile_app_events`

Smaller tables (dimensions, bridges, aggregates, staging) do NOT get `require_partition_filter` to avoid friction on small scans.
