-- BigQuery DDL for retail.fact_refunds
-- Source: retail.fact_refunds (Hive managed table, acme-analytics cluster)
-- Partition: refund_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_refunds` (
  refund_id       INT64,
  payment_id      INT64,
  return_id       INT64,
  customer_sk     INT64,
  amount          NUMERIC(14,2),
  currency_code   STRING,
  refund_ts       TIMESTAMP,
  refund_method   STRING,
  refund_date     DATE
)
PARTITION BY refund_date
OPTIONS (
  description = 'Financial refund settlements. Source: retail.fact_refunds (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
