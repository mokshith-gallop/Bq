-- BigQuery DDL for retail.dim_store_history
-- Source: retail.dim_store_history (Hive managed table, acme-analytics cluster)
-- Non-ACID SCD-2 store history (remodeling, manager changes, etc.)
-- No partition in source — plain managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_store_history` (
  history_id          INT64,
  store_sk            INT64,
  store_type          STRING,
  manager_employee_sk INT64,
  sq_ft               INT64,
  eff_from            DATE,
  eff_to              DATE,
  is_current          BOOL,
  change_reason       STRING
)
OPTIONS (
  description = 'Non-ACID SCD-2 store history. Source: retail.dim_store_history (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
