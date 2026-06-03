-- BigQuery DDL for retail.sales_cube
-- Source: retail.sales_cube (Hive managed table, acme-analytics cluster)
-- Executive daily dashboard cube table
-- Conversion: TINYINT dim_level → INT64, SMALLINT month_key → INT64
-- Partition: as_of_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.sales_cube` (
  dim_level       INT64,
  cube_key        STRING,
  country         STRING,
  month_key       INT64,
  product_sk      INT64,
  orders          INT64,
  revenue         NUMERIC(18,2),
  units           INT64,
  as_of_date      DATE
)
PARTITION BY as_of_date
OPTIONS (
  description = 'Executive daily dashboard cube (GROUPING SETS built). Source: retail.sales_cube (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
