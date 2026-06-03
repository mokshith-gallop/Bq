-- BigQuery DDL for retail.fact_returns
-- Source: retail.fact_returns (Hive managed table, acme-analytics cluster)
-- Partition: return_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_returns` (
  return_id       INT64,
  invoice_no      STRING,
  customer_sk     INT64,
  product_sk      INT64,
  return_ts       TIMESTAMP,
  quantity        INT64,
  refund_amount   NUMERIC(12,2),
  reason_code     STRING,
  return_channel  STRING,
  store_sk        INT64,
  return_date     DATE
)
PARTITION BY return_date
OPTIONS (
  description = 'Returns processed fact (non-ACID). Source: retail.fact_returns (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')]
);
