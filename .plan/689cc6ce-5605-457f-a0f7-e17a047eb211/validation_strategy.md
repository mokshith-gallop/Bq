# Validation Strategy

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
