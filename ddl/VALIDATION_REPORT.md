# DDL Validation Report

**Generated**: Hive → BigQuery DDL conversion for all 4 databases  
**Scope**: 100 tables + 15 views + 2 supporting files = 117 total files

## Summary

| Section | Check | Result |
|---------|-------|--------|
| §1 | Coverage | **PASS** |
| §2 | Structural | **PASS** |
| §3 | Type Mapping | **PASS** |
| §4 | Partitioning & Clustering | **PASS** |
| §5 | View Dialect | **PASS** |
| §6 | Cross-Reference Integrity | **PASS** |
| §7 | AC Traceability (10/10) | **PASS** |

**Overall: ALL CHECKS PASS — 0 failures**

---

## §1 Coverage Check

| Dataset | Tables | Views | Expected Tables |
|---------|--------|-------|-----------------|
| raw | 19 | 2 | 19 |
| staging | 10 | 1 | 10 |
| retail | 58 | 11 | 58 (54 Hive + 4 Kudu snapshots) |
| regional_eu | 13 | 1 | 13 |
| **Total** | **100** | **15** | **100** |

Supporting files: `deploy_order.txt`, `variables.env` (2)  
Grand total: **117 files**

## §2 Structural Checks

- ✅ No Hive-isms in DDL statements (STORED AS, TBLPROPERTIES, ROW FORMAT, LOCATION, INTO BUCKETS, EXTERNAL TABLE)
- ✅ All 100 table files use `CREATE TABLE IF NOT EXISTS`
- ✅ All 15 view files use `CREATE OR REPLACE VIEW`
- ✅ All 115 DDL files end with `;`
- ✅ All 100 table files have `OPTIONS` with `description`, `labels`, `source_system=cloudera`, `migration_tier`

## §3 Type Mapping Compliance

- ✅ No bare `INT` (all widened to `INT64`)
- ✅ No `BOOLEAN` (all mapped to `BOOL`)
- ✅ No `DECIMAL` (all mapped to `NUMERIC(p,s)`)
- ✅ No `MAP<` type (all mapped to `JSON`)
- ✅ `ARRAY<STRUCT<...>>` preserved in 4 tables (supplier_invoices, mobile_events, fact_shipments, fact_email_engagement)
- ✅ `STRUCT<...>` preserved in 7 tables (mobile_events, driver_logs, email_campaign_clicks, dim_warehouse, dim_supplier, fact_app_clicks, fact_mobile_app_events)
- ✅ `ARRAY<STRING>` preserved (fraud_signals, fraud_scored, dim_supplier, dim_promotion, dim_product_eu_catalog)
- ✅ `JSON` type in 11 tables: product_catalog_feed, email_campaign_clicks, driver_logs, mobile_events, parsed_loyalty_events, dim_store, dim_promotion, fact_app_clicks, fact_loyalty_events, fact_mobile_app_events, events_eu (column name only)

## §4 Partitioning & Clustering Compliance

- ✅ 20 tables with STRING partitions (date_ts/signal_date/signup_date/feed_date/event_date) have `partition_date DATE` added + `PARTITION BY partition_date` + original STRING column preserved
- ✅ 9 multi-column partition tables use generated columns (`DATE AS (...)` or `PARSE_DATE AS (...)`)
- ✅ No `INTO N BUCKETS` clause in any DDL statement
- ✅ 5 ACID tables have `CLUSTER BY`, no `PARTITION BY`, no `transactional` property
- ✅ Exactly 11 tables have `require_partition_filter=TRUE`:
  - raw: sales_retail, omniture_logs, pos_transactions, mobile_events
  - retail: fact_sales, fact_web_session, fact_payments, fact_shipments, fact_inventory_movements
  - regional_eu: fact_orders_eu, fact_mobile_app_events

### ACID Table Cluster Columns
| Table | CLUSTER BY |
|-------|-----------|
| returns_ledger | return_id |
| acid_customer_address_history | customer_sk |
| acid_supplier_terms_history | supplier_sk |
| acid_loyalty_points_ledger | member_id |
| acid_inventory_adjustments_log | adjustment_id |

## §5 View Dialect Compliance

- ✅ No `DATEDIFF(` (2-arg form) — replaced with `DATE_DIFF(..., DAY)`
- ✅ No `DATE_FORMAT(` — replaced with `FORMAT_DATE(...)`
- ✅ No `MONTHS_BETWEEN(` — replaced with `DATE_DIFF(..., MONTH)`
- ✅ No `NDV(` — replaced with `APPROX_COUNT_DISTINCT()`
- ✅ No `GROUPING__ID` — replaced with `GROUPING_ID(col1, col2)`
- ✅ No `WITH ROLLUP` in DDL — replaced with `GROUP BY ROLLUP(...)`
- ✅ No `unix_timestamp(` — replaced with `UNIX_SECONDS()`

### Dialect Translation Summary
| View | Translations Applied |
|------|---------------------|
| v_fraud_signals_recent | `date_format(date_sub(...))` → `FORMAT_DATE('%Y%m%d', DATE_SUB(..., INTERVAL 1 DAY))` |
| v_returns_pending | `DATEDIFF(current_date(), to_date(...))` → `DATE_DIFF(CURRENT_DATE(), DATE(...), DAY)` |
| vw_customer_lifetime_value | `DATEDIFF` → `DATE_DIFF(..., DAY)` (5 instances) |
| vw_monthly_cohort_retention | `DATE_FORMAT` → `FORMAT_DATE`, `MONTHS_BETWEEN` → `DATE_DIFF(..., MONTH)`, `to_date(concat(...))` → `PARSE_DATE(...)` |
| vw_active_member_panel | `NDV()` → `APPROX_COUNT_DISTINCT()`, `date_sub` → `DATE_SUB(..., INTERVAL)` |
| vw_sales_rollup_by_region | `GROUPING__ID` → `GROUPING_ID(...)`, `WITH ROLLUP` → `GROUP BY ROLLUP(...)` |
| vw_session_to_order_attribution | `INTERVAL '1' DAY` → `INTERVAL 1 DAY`, cross-dataset refs |
| vw_panel_continuity_score | `normalize_country(x)` → `` `${PROJECT_US}.${DS_UDFS}.normalize_country`(x) `` |
| vw_otd_by_carrier_30d | `unix_timestamp(ts)` → `UNIX_SECONDS(ts)`, `INTERVAL '48' HOUR` → `INTERVAL 48 HOUR` |
| v_eu_orders_with_consent | All refs → `${PROJECT_EU}.${DS_REGIONAL}` |

## §6 Cross-Reference Integrity

- ✅ All 115 .sql files appear exactly once in `deploy_order.txt`
- ✅ All `${...}` variables are from the defined set of 7 (PROJECT_US, PROJECT_EU, DS_RAW, DS_STAGING, DS_RETAIL, DS_REGIONAL, DS_UDFS)
- ✅ Regional files use only `PROJECT_EU`; non-regional files use only `PROJECT_US`
- ✅ All tables listed before all views in `deploy_order.txt`

## §7 Acceptance Criteria Traceability

| AC# | Criterion | Files Verified | Result |
|-----|-----------|---------------|--------|
| AC-1 | STRING partition → DATE + preserved original | raw/tables/sales_retail.sql (and 19 other date_ts tables) | ✅ PASS |
| AC-2 | Multi-column → single DATE + CLUSTER BY | fact_inventory_movements, fact_payments, fact_shipments, fact_orders_eu | ✅ PASS |
| AC-3 | Bucketing → CLUSTER BY, no INTO N BUCKETS | fact_sales (customer_sk), returns_ledger (return_id), + 6 others | ✅ PASS |
| AC-4 | MAP → JSON | mobile_events, email_campaign_clicks, fact_app_clicks, fact_mobile_app_events, + 7 others | ✅ PASS |
| AC-5 | ARRAY<STRUCT> preserved | supplier_invoices, fact_shipments, mobile_events | ✅ PASS |
| AC-6 | ACID → standard managed table | 5 ACID tables: standard BQ, CLUSTER BY, no transactional | ✅ PASS |
| AC-7 | Kudu snapshot tables exist | inventory_realtime_snapshot, session_state_snapshot, promo_eligibility_snapshot, realtime_price_snapshot | ✅ PASS |
| AC-8 | 3+1 views dialect-translated | omniture, v_fraud_signals_recent, v_returns_pending, v_eu_orders_with_consent | ✅ PASS |
| AC-9 | 6+ retail views dialect-translated | 11 retail views with DECODE→CASE, NDV→APPROX_COUNT_DISTINCT, etc. | ✅ PASS |
| AC-10 | require_partition_filter on raw.sales_retail | raw/tables/sales_retail.sql has `require_partition_filter = TRUE` | ✅ PASS |
