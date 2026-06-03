-- BigQuery DDL for retail.agg_daily_sales_by_product
-- Source: retail.agg_daily_sales_by_product (Hive managed table, acme-analytics cluster)
-- Partition: sale_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_daily_sales_by_product` (
  product_sk          INT64,
  units_sold          INT64,
  gross_revenue       NUMERIC(16,2),
  margin_pct          NUMERIC(6,4),
  cogs                NUMERIC(16,2),
  return_units        INT64,
  net_units           INT64,
  sale_date           DATE
)
PARTITION BY sale_date
OPTIONS (
  description = 'Daily sales aggregate by product. Source: retail.agg_daily_sales_by_product (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
