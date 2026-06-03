-- BigQuery DDL for retail.dim_color
-- Source: retail.dim_color (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_color` (
  color_sk        INT64,
  color_code      STRING,
  color_name      STRING,
  color_family    STRING,
  hex_code        STRING
)
OPTIONS (
  description = 'Product color attribute dimension. Source: retail.dim_color (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
