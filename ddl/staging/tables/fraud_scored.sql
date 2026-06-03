-- BigQuery DDL for staging.fraud_scored
-- Source: staging.fraud_scored (Hive managed table, acme-lake cluster)
-- Conversion: ARRAY<STRING> signals preserved as-is
-- Partition: score_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.fraud_scored` (
  txn_id         INT64,
  customer_id    STRING,
  fraud_score    NUMERIC(5,4),
  risk_band      STRING,
  signals        ARRAY<STRING>,
  scored_at      TIMESTAMP,
  score_date     DATE
)
PARTITION BY score_date
OPTIONS (
  description = 'Fraud-scored transactions — joined POS + clickstream + fraud_signals. Source: staging.fraud_scored (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
