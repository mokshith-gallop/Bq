-- BigQuery DDL for retail.fact_app_clicks
-- Source: retail.fact_app_clicks (Hive managed table, acme-analytics cluster)
-- Conversion: MAP<STRING,STRING> properties → JSON
-- Conversion: STRUCT<...> device preserved as-is
-- Partition: event_date DATE + platform_partition STRING → PARTITION BY event_date + CLUSTER BY platform_partition

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_app_clicks` (
  session_id      STRING,
  user_sk         INT64,
  event_ts        TIMESTAMP,
  event_type      STRING,
  screen          STRING,
  target_id       STRING,
  properties      JSON,
  device          STRUCT<platform STRING, version STRING, model STRING>,
  event_date      DATE,
  platform_partition STRING
)
PARTITION BY event_date
CLUSTER BY platform_partition
OPTIONS (
  description = 'Mobile/web clickstream (sessionized). Source: retail.fact_app_clicks (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
