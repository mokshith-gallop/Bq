-- BigQuery DDL for raw.returns_cdc
-- Source: raw.returns_cdc (Hive external table, acme-lake cluster)
-- Partition: snapshot_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.returns_cdc` (
  return_id      INT64,
  invoice_no     STRING,
  customer_sk    INT64,
  return_ts      TIMESTAMP,
  refund_amount  NUMERIC(12,2),
  reason_code    STRING,
  status         STRING,
  op             STRING,
  snapshot_date  DATE
)
PARTITION BY snapshot_date
OPTIONS (
  description = 'Returns CDC feed (Debezium/GoldenGate). Source: raw.returns_cdc (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
