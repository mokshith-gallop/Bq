-- BigQuery DDL for retail.dim_payment_method
-- Source: retail.dim_payment_method (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_payment_method` (
  payment_method_sk INT64,
  method_code       STRING,
  method_name       STRING,
  category          STRING,
  fee_pct           NUMERIC(5,4),
  fee_flat          NUMERIC(8,2),
  settlement_days   INT64
)
OPTIONS (
  description = 'Payment method dimension. Source: retail.dim_payment_method (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
