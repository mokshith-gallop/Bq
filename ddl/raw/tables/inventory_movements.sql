-- BigQuery DDL for raw.inventory_movements
-- Source: raw.inventory_movements (Hive external table, acme-lake cluster)
-- Conversion: multi-column partition (year/month/day INT) → partition_date DATE via generated column
-- Original INT columns preserved as regular columns
-- CLUSTER BY region per locked partitioning decision

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.inventory_movements` (
  movement_id    INT64,
  sku            STRING,
  warehouse_id   STRING,
  bin_location   STRING,
  movement_type  STRING,
  quantity       INT64,
  movement_ts    TIMESTAMP,
  reference_doc  STRING,
  operator_id    STRING,
  reason_code    STRING,
  region         STRING,
  year           INT64,
  month          INT64,
  day            INT64,
  partition_date DATE AS (DATE(year, month, day))
)
PARTITION BY partition_date
CLUSTER BY region
OPTIONS (
  description = 'Raw inventory movements (receiving, picking, shipping, adjustments). Source: raw.inventory_movements (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
