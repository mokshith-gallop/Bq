-- BigQuery DDL for retail.dim_brand
-- Source: retail.dim_brand (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_brand` (
  brand_sk        INT64,
  brand_id        STRING,
  brand_name      STRING,
  parent_company  STRING,
  private_label   BOOL,
  launch_dt       DATE
)
OPTIONS (
  description = 'Brand dimension. Source: retail.dim_brand (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
