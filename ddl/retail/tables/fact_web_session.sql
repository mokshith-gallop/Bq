-- BigQuery DDL for retail.fact_web_session
-- Source: retail.fact_web_session (Hive managed table, acme-analytics cluster)
-- Conversion: multi-column partition (event_date DATE, country STRING) →
--   PARTITION BY event_date + CLUSTER BY country

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_web_session` (
  event_ts       TIMESTAMP,
  ip             STRING,
  url            STRING,
  user_id        STRING,
  city           STRING,
  state          STRING,
  event_date     DATE,
  country        STRING
)
PARTITION BY event_date
CLUSTER BY country
OPTIONS (
  description = 'Web session fact from Omniture. Source: retail.fact_web_session (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
