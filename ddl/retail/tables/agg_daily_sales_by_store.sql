-- BigQuery DDL for retail.agg_daily_sales_by_store
-- Source: retail.agg_daily_sales_by_store (Hive managed table, acme-analytics cluster)
-- Partition: sale_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_daily_sales_by_store` (
  store_sk            INT64,
  gross_revenue       NUMERIC(16,2),
  net_revenue         NUMERIC(16,2),
  units_sold          INT64,
  txn_count           INT64,
  avg_basket          NUMERIC(12,2),
  sale_date           DATE
)
PARTITION BY sale_date
OPTIONS (
  description = 'Daily sales aggregate by store. Source: retail.agg_daily_sales_by_store (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
