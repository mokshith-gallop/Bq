-- BigQuery DDL for regional.dim_customer_snapshot
-- Source: regional.dim_customer_snapshot (Hive managed table, acme-edge cluster)
-- Sqoop landing target refreshed nightly from acme-analytics retail.dim_customer
-- No partition in source — plain managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.dim_customer_snapshot` (
  customer_sk    INT64,
  customer_id    STRING,
  country        STRING,
  snapshot_date  DATE
)
OPTIONS (
  description = 'Sqoop-refreshed customer dimension snapshot from acme-analytics. Source: regional.dim_customer_snapshot (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
