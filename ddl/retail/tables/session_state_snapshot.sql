-- BigQuery DDL for retail.session_state_snapshot
-- Source: retail.kudu_session_state (Kudu table, acme-analytics cluster)
-- Kudu batch analytics snapshot — per locked kudu_realtime_migration decision
-- Conversion: PRIMARY KEY dropped (BigQuery has no PK enforcement)
-- Conversion: PARTITION BY HASH dropped (no BQ equivalent)
-- Conversion: BIGINT started_ts, last_event_ts (epoch ms) → TIMESTAMP
-- Conversion: DECIMAL(12,2) → NUMERIC(12,2)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.session_state_snapshot` (
  session_id      STRING,
  user_id         STRING,
  started_ts      TIMESTAMP,
  last_event_ts   TIMESTAMP,
  cart_value      NUMERIC(12,2),
  cart_items      INT64,
  current_screen  STRING,
  platform        STRING,
  geo_country     STRING
)
OPTIONS (
  description = 'Kudu session state snapshot for batch analytics. Source: retail.kudu_session_state (Kudu, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
