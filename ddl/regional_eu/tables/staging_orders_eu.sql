-- BigQuery DDL for regional.staging_orders_eu
-- Source: regional.staging_orders_eu (Hive managed table, acme-edge cluster)
-- Partition: snapshot_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.staging_orders_eu` (
  order_id        STRING,
  customer_id     STRING,
  sku             STRING,
  quantity        INT64,
  unit_price      NUMERIC(10,2),
  currency_code   STRING,
  order_ts        TIMESTAMP,
  op              STRING,
  snapshot_date   DATE
)
PARTITION BY snapshot_date
OPTIONS (
  description = 'Sqoop landing for EU order CDC. Source: regional.staging_orders_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
