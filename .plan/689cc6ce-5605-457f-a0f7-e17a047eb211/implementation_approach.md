# Implementation Approach

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
