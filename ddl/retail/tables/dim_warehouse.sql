-- BigQuery DDL for retail.dim_warehouse
-- Source: retail.dim_warehouse (Hive managed table, acme-analytics cluster)
-- Conversion: STRUCT<...> geocode preserved as-is (DOUBLE→FLOAT64)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_warehouse` (
  warehouse_sk    INT64,
  warehouse_id    STRING,
  name            STRING,
  type            STRING,
  operator        STRING,
  region          STRING,
  capacity_units  INT64,
  open_dt         DATE,
  geocode         STRUCT<lat FLOAT64, lon FLOAT64>
)
OPTIONS (
  description = 'Warehouse/distribution center dimension. Source: retail.dim_warehouse (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
