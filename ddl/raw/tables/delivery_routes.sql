-- BigQuery DDL for raw.delivery_routes
-- Source: raw.delivery_routes (Hive CSV external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.delivery_routes` (
  route_id       STRING,
  driver_id      STRING,
  vehicle_id     STRING,
  planned_stops  INT64,
  actual_stops   INT64,
  miles_driven   NUMERIC(8,2),
  fuel_used      NUMERIC(8,2),
  start_ts       TIMESTAMP,
  end_ts         TIMESTAMP,
  date_ts        STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Delivery route plans. Source: raw.delivery_routes (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
