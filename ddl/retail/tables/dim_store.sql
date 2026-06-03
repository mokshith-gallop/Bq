-- BigQuery DDL for retail.dim_store
-- Source: retail.dim_store (Hive managed table, acme-analytics cluster)
-- Conversion: MAP<STRING,STRING> attributes → JSON
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_store` (
  store_sk            INT64,
  store_id            STRING,
  store_name          STRING,
  store_type          STRING,
  region              STRING,
  city                STRING,
  state               STRING,
  country             STRING,
  open_dt             DATE,
  close_dt            DATE,
  sq_ft               INT64,
  manager_employee_sk INT64,
  attributes          JSON
)
OPTIONS (
  description = 'Store dimension with location attributes. Source: retail.dim_store (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
