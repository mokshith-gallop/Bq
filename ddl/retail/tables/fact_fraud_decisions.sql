-- BigQuery DDL for retail.fact_fraud_decisions
-- Source: retail.fact_fraud_decisions (Hive managed table, acme-analytics cluster)
-- Conversion: ARRAY<STRING> rule_signals preserved as-is
-- Partition: decision_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_fraud_decisions` (
  txn_id          INT64,
  customer_sk     INT64,
  fraud_score     NUMERIC(5,4),
  decision        STRING,
  rule_signals    ARRAY<STRING>,
  decided_ts      TIMESTAMP,
  decision_date   DATE
)
PARTITION BY decision_date
OPTIONS (
  description = 'Fraud engine decision outcomes fact. Source: retail.fact_fraud_decisions (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
