-- BigQuery DDL for retail.dim_employee
-- Source: retail.dim_employee (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_employee` (
  employee_sk     INT64,
  employee_id     STRING,
  first_name      STRING,
  last_name       STRING,
  hire_dt         DATE,
  termination_dt  DATE,
  role            STRING,
  department      STRING,
  home_store_sk   INT64,
  manager_sk      INT64,
  salary_band     STRING
)
OPTIONS (
  description = 'Employee dimension (store, warehouse, corporate). Source: retail.dim_employee (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
