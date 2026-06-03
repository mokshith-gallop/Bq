-- BigQuery DDL for retail.dim_size
-- Source: retail.dim_size (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_size` (
  size_sk         INT64,
  size_code       STRING,
  size_name       STRING,
  size_system     STRING,
  sort_order      INT64
)
OPTIONS (
  description = 'Size variant attribute dimension. Source: retail.dim_size (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
