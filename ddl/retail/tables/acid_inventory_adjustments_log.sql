-- BigQuery DDL for retail.acid_inventory_adjustments_log
-- Source: retail.acid_inventory_adjustments_log (Hive ACID/ORC table, acme-analytics cluster)
-- Conversion: transactional=true → standard BigQuery managed table (BQ supports native UPDATE/DELETE/MERGE)
-- Conversion: CLUSTERED BY (adjustment_id) INTO 4 BUCKETS → CLUSTER BY adjustment_id
-- No PARTITION BY (non-partitioned ACID table in source)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.acid_inventory_adjustments_log` (
  adjustment_id   INT64,
  warehouse_sk    INT64,
  sku             STRING,
  quantity_delta  INT64,
  reason_code     STRING,
  notes           STRING,
  adjusted_by     STRING,
  adjusted_at     TIMESTAMP,
  approved_by     STRING,
  approved_at     TIMESTAMP
)
CLUSTER BY adjustment_id
OPTIONS (
  description = 'Manual inventory adjustments audit log (ACID source — standard BQ managed table). Source: retail.acid_inventory_adjustments_log (Hive ACID, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
