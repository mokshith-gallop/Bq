-- BigQuery DDL for retail.dim_employee_history
-- Source: retail.dim_employee_history (Hive managed table, acme-analytics cluster)
-- Non-ACID SCD-2 employee history
-- Partition: eff_from_year INT — preserved as regular column, no date partition needed

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_employee_history` (
  history_id      INT64,
  employee_sk     INT64,
  role            STRING,
  department      STRING,
  home_store_sk   INT64,
  salary_band     STRING,
  eff_from        DATE,
  eff_to          DATE,
  is_current      BOOL,
  eff_from_year   INT64
)
OPTIONS (
  description = 'Non-ACID SCD-2 employee history. Source: retail.dim_employee_history (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
