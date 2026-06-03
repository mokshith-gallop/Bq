-- BigQuery DDL for raw.return_authorizations
-- Source: raw.return_authorizations (Hive TSV external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.return_authorizations` (
  rma_id          STRING,
  customer_id     STRING,
  invoice_no      STRING,
  stock_code      STRING,
  quantity        INT64,
  reason_code     STRING,
  reason_text     STRING,
  requested_at    TIMESTAMP,
  approved        BOOL,
  refund_amount   NUMERIC(12,2),
  date_ts         STRING,
  partition_date  DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Return authorization requests (RMA tickets). Source: raw.return_authorizations (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
