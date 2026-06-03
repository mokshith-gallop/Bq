-- BigQuery DDL for regional.fact_mobile_app_events
-- Source: regional.fact_mobile_app_events (Hive managed table, acme-edge cluster)
-- Conversion: MAP<STRING,STRING> properties → JSON
-- Conversion: STRUCT<...> device preserved as-is
-- Conversion: event_date STRING partition → partition_date DATE via generated column
-- Conversion: platform_partition STRING partition → CLUSTER BY

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.fact_mobile_app_events` (
  event_id        STRING,
  session_id      STRING,
  user_id         STRING,
  event_type      STRING,
  event_ts        TIMESTAMP,
  screen          STRING,
  device          STRUCT<platform STRING, model STRING, os_version STRING>,
  properties      JSON,
  country_iso2    STRING,
  event_date      STRING,
  platform_partition STRING,
  partition_date  DATE AS (PARSE_DATE('%Y%m%d', event_date))
)
PARTITION BY partition_date
CLUSTER BY platform_partition
OPTIONS (
  description = 'Mobile app events for EU users. Source: regional.fact_mobile_app_events (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
