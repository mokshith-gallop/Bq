-- BigQuery DDL for retail.fact_chat_interactions
-- Source: retail.fact_chat_interactions (Hive managed table, acme-analytics cluster)
-- Partition: start_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_chat_interactions` (
  chat_id          STRING,
  customer_sk      INT64,
  agent_sk         INT64,
  started_at       TIMESTAMP,
  ended_at         TIMESTAMP,
  duration_sec     INT64,
  message_count    INT64,
  resolved         BOOL,
  csat_score       INT64,
  sentiment_avg    NUMERIC(4,3),
  start_date       DATE
)
PARTITION BY start_date
OPTIONS (
  description = 'Customer service chat metrics. Source: retail.fact_chat_interactions (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
