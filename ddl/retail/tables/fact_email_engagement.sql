-- BigQuery DDL for retail.fact_email_engagement
-- Source: retail.fact_email_engagement (Hive managed table, acme-analytics cluster)
-- Conversion: ARRAY<STRUCT<...>> clicks preserved as-is
-- Partition: event_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_email_engagement` (
  send_id         STRING,
  campaign_sk     INT64,
  user_sk         INT64,
  event_type      STRING,
  event_ts        TIMESTAMP,
  link_url        STRING,
  clicks          ARRAY<STRUCT<ts TIMESTAMP, url STRING>>,
  event_date      DATE
)
PARTITION BY event_date
OPTIONS (
  description = 'Email open/click/unsubscribe events. Source: retail.fact_email_engagement (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
