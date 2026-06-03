-- BigQuery DDL for staging.normalized_carrier_events
-- Source: staging.normalized_carrier_events (Hive managed table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.normalized_carrier_events` (
  tracking_no      STRING,
  carrier          STRING,
  event_type       STRING,
  event_ts         TIMESTAMP,
  location_city    STRING,
  location_region  STRING,
  location_country STRING,
  date_ts          STRING,
  partition_date   DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Normalized carrier events — unifies UPS/FedEx/DHL semantics. Source: staging.normalized_carrier_events (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
