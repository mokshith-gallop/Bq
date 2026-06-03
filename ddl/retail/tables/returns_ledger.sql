-- BigQuery DDL for retail.returns_ledger
-- Source: retail.returns_ledger (Hive ACID/ORC table, acme-analytics cluster)
-- Conversion: transactional=true → standard BigQuery managed table (BQ supports native UPDATE/DELETE/MERGE)
-- Conversion: CLUSTERED BY (return_id) INTO 4 BUCKETS → CLUSTER BY return_id
-- No PARTITION BY (non-partitioned ACID table in source)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.returns_ledger` (
  return_id       INT64,
  invoice_no      STRING,
  customer_sk     INT64,
  return_ts       TIMESTAMP,
  refund_amount   NUMERIC(12,2),
  reason_code     STRING,
  status          STRING
)
CLUSTER BY return_id
OPTIONS (
  description = 'Returns ledger (ACID source — standard BQ managed table). Source: retail.returns_ledger (Hive ACID, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')]
);
