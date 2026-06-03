-- BigQuery DDL for retail.fact_loyalty_events
-- Source: retail.fact_loyalty_events (Hive managed table, acme-analytics cluster)
-- Conversion: MAP<STRING,STRING> meta → JSON
-- Partition: event_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_loyalty_events` (
  event_id        INT64,
  member_id       STRING,
  event_type      STRING,
  points          INT64,
  store_sk        INT64,
  tx_id           STRING,
  event_ts        TIMESTAMP,
  meta            JSON,
  event_date      DATE
)
PARTITION BY event_date
OPTIONS (
  description = 'Loyalty point earn/redeem events. Source: retail.fact_loyalty_events (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
