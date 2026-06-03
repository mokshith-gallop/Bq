-- BigQuery DDL for retail.promo_eligibility_snapshot
-- Source: retail.kudu_promo_eligibility (Kudu table, acme-analytics cluster)
-- Kudu batch analytics snapshot — per locked kudu_realtime_migration decision
-- Conversion: PRIMARY KEY dropped (BigQuery has no PK enforcement)
-- Conversion: PARTITION BY HASH dropped (no BQ equivalent)
-- Conversion: BIGINT valid_from_ts, valid_to_ts (epoch ms) → TIMESTAMP
-- Conversion: BOOLEAN → BOOL

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.promo_eligibility_snapshot` (
  customer_id        STRING,
  promo_id           STRING,
  eligible           BOOL,
  eligibility_reason STRING,
  valid_from_ts      TIMESTAMP,
  valid_to_ts        TIMESTAMP,
  redeemed           BOOL
)
OPTIONS (
  description = 'Kudu promo eligibility snapshot for batch analytics. Source: retail.kudu_promo_eligibility (Kudu, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
