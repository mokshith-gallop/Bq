-- BigQuery DDL for staging.geocoded_addresses
-- Source: staging.geocoded_addresses (Hive managed table, acme-lake cluster)
-- Partition: load_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.geocoded_addresses` (
  raw_addr_hash  STRING,
  addr_line1     STRING,
  addr_city      STRING,
  addr_region    STRING,
  addr_country   STRING,
  addr_postal    STRING,
  lat            FLOAT64,
  lon            FLOAT64,
  confidence     NUMERIC(4,3),
  provider       STRING,
  load_date      DATE
)
PARTITION BY load_date
OPTIONS (
  description = 'Geocoded addresses — address standardization output. Source: staging.geocoded_addresses (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
