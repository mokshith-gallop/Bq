-- BigQuery DDL for regional.dim_currency_eu
-- Source: regional.dim_currency_eu (Hive managed table, acme-edge cluster)
-- No partition — plain managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.dim_currency_eu` (
  currency_code   STRING,
  currency_name   STRING,
  minor_unit      INT64,
  symbol          STRING,
  eurozone        BOOL
)
OPTIONS (
  description = 'EU-relevant currencies dimension (subset of global). Source: regional.dim_currency_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
