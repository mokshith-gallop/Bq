-- BigQuery DDL for retail.top_countries_daily
-- Source: retail.top_countries_daily (Hive managed table, acme-analytics cluster)
-- Conversion: TINYINT rank → INT64
-- No partition in source — plain managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.top_countries_daily` (
  as_of_date   DATE,
  country      STRING,
  orders       INT64,
  revenue      NUMERIC(18,2),
  rank         INT64
)
OPTIONS (
  description = 'Top countries daily ranking. Source: retail.top_countries_daily (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
