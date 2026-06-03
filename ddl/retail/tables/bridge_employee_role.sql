-- BigQuery DDL for retail.bridge_employee_role
-- Source: retail.bridge_employee_role (Hive managed table, acme-analytics cluster)
-- Non-partitioned bridge table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.bridge_employee_role` (
  employee_sk     INT64,
  role            STRING,
  primary_role    BOOL,
  eff_from        DATE,
  eff_to          DATE
)
OPTIONS (
  description = 'Employee M:N role assignments bridge. Source: retail.bridge_employee_role (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
