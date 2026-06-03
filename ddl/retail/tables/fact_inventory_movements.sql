-- BigQuery DDL for retail.fact_inventory_movements
-- Source: retail.fact_inventory_movements (Hive managed table, acme-analytics cluster)
-- Conversion: multi-column partition (year/month/day INT, region STRING) →
--   partition_date DATE via generated column + CLUSTER BY region
-- Conversion: CLUSTERED BY (sku) INTO 32 BUCKETS → dropped (region in CLUSTER BY per locked decision)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_inventory_movements` (
  movement_id    INT64,
  movement_ts    TIMESTAMP,
  sku            STRING,
  warehouse_sk   INT64,
  store_sk       INT64,
  movement_type  STRING,
  quantity       INT64,
  reference_doc  STRING,
  reason_code    STRING,
  operator_sk    INT64,
  year           INT64,
  month          INT64,
  day            INT64,
  region         STRING,
  partition_date DATE AS (DATE(year, month, day))
)
PARTITION BY partition_date
CLUSTER BY region
OPTIONS (
  description = 'High-volume inventory movement fact. Source: retail.fact_inventory_movements (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
