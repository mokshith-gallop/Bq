-- BigQuery DDL for raw.mobile_events
-- Source: raw.mobile_events (Hive JSON SerDe external table, acme-lake cluster)
-- Conversion: MAP<STRING,STRING> properties → JSON
-- Conversion: STRUCT<...> context preserved as-is
-- Conversion: ARRAY<STRUCT<...>> items preserved as-is
-- Conversion: TINYINT hour_bucket → INT64
-- Conversion: event_date STRING partition → PARTITION BY PARSE_DATE
-- Conversion: hour_bucket secondary partition → CLUSTER BY

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.mobile_events` (
  event_id        STRING,
  event_ts        TIMESTAMP,
  user_id         STRING,
  app_version     STRING,
  device_type     STRING,
  platform        STRING,
  properties      JSON,
  context         STRUCT<
                    ip STRING,
                    country STRING,
                    session_id STRING,
                    referrer STRING
                  >,
  items           ARRAY<STRUCT<sku STRING, qty INT64, price NUMERIC(10,2)>>,
  event_date      STRING,
  hour_bucket     INT64,
  partition_date  DATE AS (PARSE_DATE('%Y%m%d', event_date))
)
PARTITION BY partition_date
CLUSTER BY hour_bucket
OPTIONS (
  description = 'Raw mobile app event feeds (NDJSON via Flume). Source: raw.mobile_events (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
