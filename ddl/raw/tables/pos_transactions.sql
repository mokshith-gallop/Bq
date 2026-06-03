-- BigQuery DDL for raw.pos_transactions
-- Source: raw.pos_transactions (Hive external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.pos_transactions` (
  txn_id          INT64,
  store_id        STRING,
  register_id     STRING,
  cashier_id      STRING,
  customer_id     STRING,
  invoice_no      STRING,
  txn_ts          TIMESTAMP,
  line_count      INT64,
  gross_amount    NUMERIC(14,2),
  discount_amount NUMERIC(14,2),
  tax_amount      NUMERIC(14,2),
  tender_type     STRING,
  void_flag       BOOL,
  date_ts         STRING,
  partition_date  DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'POS transaction feed (high-volume, columnar). Source: raw.pos_transactions (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
