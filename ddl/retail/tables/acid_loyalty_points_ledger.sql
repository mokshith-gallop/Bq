-- BigQuery DDL for retail.acid_loyalty_points_ledger
-- Source: retail.acid_loyalty_points_ledger (Hive ACID/ORC table, acme-analytics cluster)
-- Conversion: transactional=true → standard BigQuery managed table (BQ supports native UPDATE/DELETE/MERGE)
-- Conversion: CLUSTERED BY (member_id) INTO 8 BUCKETS → CLUSTER BY member_id
-- No PARTITION BY (non-partitioned ACID table in source)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.acid_loyalty_points_ledger` (
  entry_id        INT64,
  member_id       STRING,
  points_delta    INT64,
  running_balance INT64,
  event_ts        TIMESTAMP,
  event_type      STRING,
  reference_id    STRING,
  expiry_ts       TIMESTAMP
)
CLUSTER BY member_id
OPTIONS (
  description = 'Live loyalty points earn/redeem ledger (ACID source — standard BQ managed table). Source: retail.acid_loyalty_points_ledger (Hive ACID, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
