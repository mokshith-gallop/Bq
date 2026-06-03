-- BigQuery DDL for staging.cleansed_orders
-- Source: staging.cleansed_orders (Hive managed table, acme-lake cluster)
-- Partition: order_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.cleansed_orders` (
  order_id       STRING,
  customer_id    STRING,
  invoice_no     STRING,
  txn_ts         TIMESTAMP,
  line_count     INT64,
  gross_amount   NUMERIC(14,2),
  discount       NUMERIC(14,2),
  tax            NUMERIC(14,2),
  net_amount     NUMERIC(14,2),
  tender_type    STRING,
  source_feed    STRING,
  order_date     DATE
)
PARTITION BY order_date
OPTIONS (
  description = 'Cleansed orders — dedup''d and typed from raw.sales_retail + pos_transactions. Source: staging.cleansed_orders (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
