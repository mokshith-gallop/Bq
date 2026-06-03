-- BigQuery DDL for raw.customer_complaints
-- Source: raw.customer_complaints (Hive TSV external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.customer_complaints` (
  complaint_id  STRING,
  customer_id   STRING,
  invoice_no    STRING,
  channel       STRING,
  severity      STRING,
  summary       STRING,
  body          STRING,
  created_at    TIMESTAMP,
  resolved_at   TIMESTAMP,
  csat_score    INT64,
  date_ts       STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Customer complaints (ticketing system extract). Source: raw.customer_complaints (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
