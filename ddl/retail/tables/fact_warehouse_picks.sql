-- BigQuery DDL for retail.fact_warehouse_picks
-- Source: retail.fact_warehouse_picks (Hive managed table, acme-analytics cluster)
-- Conversion: CLUSTERED BY (picker_sk) INTO 8 BUCKETS → CLUSTER BY picker_sk
-- Partition: pick_date DATE + warehouse_partition STRING → PARTITION BY pick_date + CLUSTER BY

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_warehouse_picks` (
  pick_id          INT64,
  warehouse_sk     INT64,
  picker_sk        INT64,
  sku              STRING,
  quantity         INT64,
  picked_ts        TIMESTAMP,
  duration_ms      INT64,
  bin_location     STRING,
  pick_date        DATE,
  warehouse_partition STRING
)
PARTITION BY pick_date
CLUSTER BY warehouse_partition, picker_sk
OPTIONS (
  description = 'Warehouse picking events (granular). Source: retail.fact_warehouse_picks (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
