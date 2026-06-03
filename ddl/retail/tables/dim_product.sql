-- BigQuery DDL for retail.dim_product
-- Source: retail.dim_product (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_product` (
  product_sk     INT64,
  stock_code     STRING,
  description    STRING,
  unit_price     NUMERIC(10,2)
)
OPTIONS (
  description = 'Product dimension. Source: retail.dim_product (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')]
);
