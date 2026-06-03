-- BigQuery DDL for raw.driver_logs
-- Source: raw.driver_logs (Hive JSON SerDe external table, acme-lake cluster)
-- Conversion: MAP<STRING,STRING> extras → JSON
-- Conversion: STRUCT<...> gps preserved as-is
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.driver_logs` (
  driver_id    STRING,
  event_ts     TIMESTAMP,
  event_type   STRING,
  gps          STRUCT<lat FLOAT64, lon FLOAT64>,
  notes        STRING,
  extras       JSON,
  date_ts      STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Driver event logs (JSON lines). Source: raw.driver_logs (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
