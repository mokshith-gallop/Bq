-- BigQuery DDL for raw.warehouse_picks
-- Source: raw.warehouse_picks (Hive Parquet external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved
-- Conversion: warehouse_id_partition STRING partition → CLUSTER BY

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.warehouse_picks` (
  pick_id        INT64,
  warehouse_id   STRING,
  bin_id         STRING,
  sku            STRING,
  picker_id      STRING,
  quantity       INT64,
  picked_at      TIMESTAMP,
  duration_ms    INT64,
  date_ts        STRING,
  warehouse_id_partition STRING,
  partition_date DATE
)
PARTITION BY partition_date
CLUSTER BY warehouse_id_partition
OPTIONS (
  description = 'Warehouse pick events. Source: raw.warehouse_picks (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
