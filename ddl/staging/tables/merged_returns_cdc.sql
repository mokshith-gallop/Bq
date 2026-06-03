-- BigQuery DDL for staging.merged_returns_cdc
-- Source: staging.merged_returns_cdc (Hive managed table, acme-lake cluster)
-- Partition: snapshot_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.merged_returns_cdc` (
  return_id      INT64,
  invoice_no     STRING,
  customer_sk    INT64,
  return_ts      TIMESTAMP,
  refund_amount  NUMERIC(12,2),
  reason_code    STRING,
  status         STRING,
  is_deleted     BOOL,
  snapshot_date  DATE
)
PARTITION BY snapshot_date
OPTIONS (
  description = 'Merged returns CDC — coalesced I/U/D ops into latest-state rows. Source: staging.merged_returns_cdc (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
