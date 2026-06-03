-- BigQuery DDL for raw.email_campaign_clicks
-- Source: raw.email_campaign_clicks (Hive JSON SerDe external table, acme-lake cluster)
-- Conversion: MAP<STRING,STRING> utm → JSON
-- Conversion: STRUCT<...> geo preserved as-is
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.email_campaign_clicks` (
  campaign_id  STRING,
  send_id      STRING,
  recipient    STRING,
  clicked_at   TIMESTAMP,
  click_url    STRING,
  user_agent   STRING,
  ip_address   STRING,
  geo          STRUCT<country STRING, region STRING, city STRING>,
  utm          JSON,
  date_ts      STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Email campaign click tracking (JSON Lines). Source: raw.email_campaign_clicks (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
