-- BigQuery DDL for retail.fact_inventory_snapshot
-- Source: retail.fact_inventory_snapshot (Hive managed table, acme-analytics cluster)
-- Conversion: CLUSTERED BY (sku) INTO 16 BUCKETS → CLUSTER BY sku
-- Partition: snapshot_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_inventory_snapshot` (
  sku              STRING,
  warehouse_sk     INT64,
  on_hand_units    INT64,
  allocated_units  INT64,
  in_transit_units INT64,
  available_units  INT64,
  avg_cost         NUMERIC(12,4),
  last_movement_ts TIMESTAMP,
  snapshot_date    DATE
)
PARTITION BY snapshot_date
CLUSTER BY sku
OPTIONS (
  description = 'End-of-day inventory state per warehouse+sku. Source: retail.fact_inventory_snapshot (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
