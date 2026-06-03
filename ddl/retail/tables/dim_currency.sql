-- BigQuery DDL for retail.dim_currency
-- Source: retail.dim_currency (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_currency` (
  currency_code   STRING,
  currency_name   STRING,
  minor_unit      INT64,
  symbol          STRING
)
OPTIONS (
  description = 'ISO currency master dimension. Source: retail.dim_currency (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
