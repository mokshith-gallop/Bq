-- BigQuery DDL for staging.parsed_loyalty_events
-- Source: staging.parsed_loyalty_events (Hive managed table, acme-lake cluster)
-- Conversion: MAP<STRING,STRING> meta → JSON
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.parsed_loyalty_events` (
  event_ts       TIMESTAMP,
  member_id      STRING,
  event_type     STRING,
  points         INT64,
  store_id       STRING,
  tx_id          STRING,
  meta           JSON,
  date_ts        STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Parsed loyalty events — output of regex-parsed raw.loyalty_events. Source: staging.parsed_loyalty_events (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
