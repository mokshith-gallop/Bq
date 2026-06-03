-- BigQuery DDL for regional.staging_customers_eu_cdc
-- Source: regional.staging_customers_eu_cdc (Hive managed table, acme-edge cluster)
-- Partition: snapshot_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.staging_customers_eu_cdc` (
  customer_id        STRING,
  email              STRING,
  first_name         STRING,
  last_name          STRING,
  country_iso2       STRING,
  addr_postal        STRING,
  consent_marketing  BOOL,
  updated_ts         TIMESTAMP,
  op                 STRING,
  snapshot_date      DATE
)
PARTITION BY snapshot_date
OPTIONS (
  description = 'CDC feed from EU customer DB. Source: regional.staging_customers_eu_cdc (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
