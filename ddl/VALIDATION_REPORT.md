# DDL Validation Report

**Date**: 2026-06-03
**Scope**: All 115 BigQuery DDL files (100 tables + 15 views) + 2 supporting files
**Result**: ✅ ALL CHECKS PASS — 0 issues found

---

## §1. Coverage Check — PASS

| Dataset | Tables | Views | Expected Tables |
|---------|--------|-------|-----------------|
| raw | 19 | 2 | 19 |
| staging | 10 | 1 | 10 |
| retail | 58 | 11 | 58 (54 Hive + 4 Kudu) |
| regional_eu | 13 | 1 | 13 |
| **Total** | **100** | **15** | **100** |

Supporting files: `deploy_order.txt`, `variables.env` — both present.
Grand total: **117 files**.

## §2. Structural Checks — PASS

| Check | Result |
|-------|--------|
| No Hive-isms (STORED AS, TBLPROPERTIES, ROW FORMAT, LOCATION, SERDE, INTO BUCKETS, EXTERNAL TABLE) | ✅ PASS |
| All tables use `CREATE TABLE IF NOT EXISTS` | ✅ PASS |
| All views use `CREATE OR REPLACE VIEW` | ✅ PASS |
| All statements end with `;` | ✅ PASS |
| Every table has OPTIONS with `description`, `labels`, `source_system`, `migration_tier` | ✅ PASS |

## §3. Type Mapping Compliance — PASS

| Check | Result |
|-------|--------|
| No bare `INT` (all INT64) | ✅ PASS |
| No `BOOLEAN` (all BOOL) | ✅ PASS |
| No `DECIMAL` (all NUMERIC) | ✅ PASS |
| No `MAP<` (all JSON) | ✅ PASS |
| JSON present in 10 expected tables | ✅ PASS |
| `ARRAY<STRUCT<...>>` preserved in 4 tables | ✅ PASS |
| `STRUCT<...>` preserved in 7+ tables | ✅ PASS |
| `ARRAY<STRING>` preserved in 6 tables | ✅ PASS |
| Kudu BIGINT epoch-ms → TIMESTAMP in 4 snapshot tables | ✅ PASS |

## §4. Partitioning & Clustering Compliance — PASS

| Check | Result |
|-------|--------|
| 16 date_ts/STRING partition tables → `partition_date DATE` + `PARTITION BY partition_date` | ✅ PASS |
| 9 multi-column partition tables → generated DATE columns | ✅ PASS |
| No `INTO N BUCKETS` in any DDL statement | ✅ PASS |
| 5 ACID tables: `CLUSTER BY` present, no `PARTITION BY`, no `transactional` | ✅ PASS |
| `require_partition_filter=TRUE` on exactly 11 specified tables | ✅ PASS |

### ACID Table Details:
| Table | CLUSTER BY Column | Bucket Source |
|-------|-------------------|---------------|
| returns_ledger | return_id | INTO 4 BUCKETS |
| acid_customer_address_history | customer_sk | INTO 8 BUCKETS |
| acid_supplier_terms_history | supplier_sk | INTO 4 BUCKETS |
| acid_loyalty_points_ledger | member_id | INTO 8 BUCKETS |
| acid_inventory_adjustments_log | adjustment_id | INTO 4 BUCKETS |

### require_partition_filter Tables (11):
1. `raw.sales_retail`
2. `raw.omniture_logs`
3. `raw.pos_transactions`
4. `raw.mobile_events`
5. `retail.fact_sales`
6. `retail.fact_web_session`
7. `retail.fact_payments`
8. `retail.fact_shipments`
9. `retail.fact_inventory_movements`
10. `regional_eu.fact_orders_eu`
11. `regional_eu.fact_mobile_app_events`

## §5. View Dialect Compliance — PASS

| Hive/Impala Construct | Check Result |
|-----------------------|-------------|
| `DATEDIFF(a,b)` (2-arg) | ✅ None found |
| `DATE_FORMAT(d, fmt)` | ✅ None found |
| `MONTHS_BETWEEN(a,b)` | ✅ None found |
| `NDV(col)` | ✅ None found |
| `GROUPING__ID` (double underscore) | ✅ None found |
| `WITH ROLLUP` (after GROUP BY) | ✅ None found |
| `unix_timestamp(ts)` | ✅ None found |
| `date_sub(d, N)` (without INTERVAL) | ✅ None found |
| `to_date(expr)` | ✅ None found |
| `ILIKE` | ✅ None found |
| `DECODE(...)` | ✅ None found |

BigQuery equivalents verified present:
- `DATE_DIFF` in 3 views
- `FORMAT_DATE` in 2 views
- `APPROX_COUNT_DISTINCT` in 1 view
- `GROUPING_ID()` in 1 view
- `GROUP BY ROLLUP()` in 1 view
- `UNIX_SECONDS` in 1 view
- `DATE_SUB(..., INTERVAL)` in 5 views
- `PARSE_DATE` in 1 view

## §6. Cross-Reference Integrity — PASS

| Check | Result |
|-------|--------|
| All 115 .sql files appear exactly once in `deploy_order.txt` | ✅ PASS |
| All `${...}` variables are from the defined set of 7 | ✅ PASS |
| Regional objects use `PROJECT_EU`, never `PROJECT_US` | ✅ PASS |
| US objects use `PROJECT_US`, never `PROJECT_EU` | ✅ PASS |
| All tables before all views in deploy order | ✅ PASS |
| All 7 variables defined in `variables.env` | ✅ PASS |

## §7. Acceptance Criteria Traceability — ALL 10 PASS

| AC# | Criterion | Verification | Result |
|-----|-----------|-------------|--------|
| AC-1 | STRING partition → DATE + preserved original | `raw/tables/sales_retail.sql`: `date_ts STRING` preserved, `partition_date DATE` added, `PARTITION BY partition_date` | ✅ PASS |
| AC-2 | Multi-column → single DATE + CLUSTER BY | 4 tables verified: fact_inventory_movements (year/month/day→DATE+region), fact_payments (post_year/post_month→DATE+payment_method_partition), fact_shipments (ship_year/ship_month/ship_day→DATE+carrier_partition), fact_orders_eu (order_year/order_month→DATE+country_partition,customer_id) | ✅ PASS |
| AC-3 | Bucketing → CLUSTER BY, no INTO N BUCKETS | fact_sales (customer_sk), returns_ledger (return_id) — zero INTO BUCKETS in any DDL | ✅ PASS |
| AC-4 | MAP → JSON | mobile_events.properties, email_campaign_clicks.utm, fact_app_clicks.properties, fact_mobile_app_events.properties — all typed as JSON | ✅ PASS |
| AC-5 | ARRAY<STRUCT> preserved | supplier_invoices.line_items, fact_shipments.tracking_events, mobile_events.items — all ARRAY<STRUCT<...>> | ✅ PASS |
| AC-6 | ACID → standard managed table | 5 ACID tables: CREATE TABLE IF NOT EXISTS, CLUSTER BY, no transactional, no PARTITION BY | ✅ PASS |
| AC-7 | Kudu snapshot tables exist | inventory_realtime_snapshot, session_state_snapshot, promo_eligibility_snapshot, realtime_price_snapshot — all present with TIMESTAMP columns | ✅ PASS |
| AC-8 | 3+1 views with translated SQL | omniture, v_fraud_signals_recent (FORMAT_DATE+DATE_SUB), v_returns_pending (DATE_DIFF+DATE), v_eu_orders_with_consent — all present | ✅ PASS |
| AC-9 | 6+ retail analytics views | 11 retail views with DATEDIFF→DATE_DIFF, DATE_FORMAT→FORMAT_DATE, MONTHS_BETWEEN→DATE_DIFF, NDV→APPROX_COUNT_DISTINCT, GROUPING__ID→GROUPING_ID, WITH ROLLUP→GROUP BY ROLLUP, normalize_country→fully-qualified UDF | ✅ PASS |
| AC-10 | require_partition_filter on sales_retail | `OPTIONS(require_partition_filter=TRUE)` confirmed | ✅ PASS |

---

## Summary

**115/115 DDL files validated. 0 issues found. All 10 acceptance criteria satisfied.**
