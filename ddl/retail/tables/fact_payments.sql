-- BigQuery DDL for retail.fact_payments
-- Source: retail.fact_payments (Hive managed table, acme-analytics cluster)
-- Conversion: multi-column partition (post_year/post_month INT, payment_method_partition STRING) →
--   partition_month DATE via generated column + CLUSTER BY payment_method_partition
-- Conversion: CLUSTERED BY (invoice_no) INTO 16 BUCKETS → dropped (payment_method_partition in CLUSTER BY per locked decision)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_payments` (
  payment_id          INT64,
  invoice_no          STRING,
  customer_sk         INT64,
  payment_method_sk   INT64,
  amount              NUMERIC(14,2),
  currency_code       STRING,
  payment_ts          TIMESTAMP,
  auth_code           STRING,
  settlement_id       STRING,
  fee_amount          NUMERIC(10,2),
  post_year           INT64,
  post_month          INT64,
  payment_method_partition STRING,
  partition_month     DATE AS (DATE(post_year, post_month, 1))
)
PARTITION BY partition_month
CLUSTER BY payment_method_partition
OPTIONS (
  description = 'Payment events fact. Source: retail.fact_payments (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
