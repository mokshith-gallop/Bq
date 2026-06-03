-- BigQuery DDL for retail.realtime_price_snapshot
-- Source: retail.kudu_realtime_price (Kudu table, acme-analytics cluster)
-- Kudu batch analytics snapshot — per locked kudu_realtime_migration decision
-- Conversion: PRIMARY KEY dropped (BigQuery has no PK enforcement)
-- Conversion: PARTITION BY HASH dropped (no BQ equivalent)
-- Conversion: BIGINT updated_ts (epoch ms) → TIMESTAMP
-- Conversion: DECIMAL → NUMERIC

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.realtime_price_snapshot` (
  sku             STRING,
  store_id        STRING,
  price           NUMERIC(10,2),
  list_price      NUMERIC(10,2),
  cost            NUMERIC(10,2),
  margin_pct      NUMERIC(5,4),
  updated_ts      TIMESTAMP,
  pricing_engine  STRING
)
OPTIONS (
  description = 'Kudu realtime price snapshot for batch analytics. Source: retail.kudu_realtime_price (Kudu, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
