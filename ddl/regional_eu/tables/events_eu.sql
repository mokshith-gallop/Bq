-- BigQuery DDL for regional.events_eu
-- Source: regional.events_eu (Hive managed table, acme-edge cluster)
-- Note: payload_json is a STRING column that holds JSON data — stays as STRING per locked decision
-- Conversion: event_date STRING partition → partition_date DATE added; original event_date preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.events_eu` (
  event_id     STRING,
  event_ts     TIMESTAMP,
  user_id      STRING,
  event_type   STRING,
  country_iso2 STRING,
  payload_json STRING,
  event_date   STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'EU regional event collection. Source: regional.events_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
