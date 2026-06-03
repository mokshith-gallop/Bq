-- BigQuery DDL for retail.bridge_promo_eligibility
-- Source: retail.bridge_promo_eligibility (Hive managed table, acme-analytics cluster)
-- Partition: load_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.bridge_promo_eligibility` (
  customer_sk    INT64,
  promo_sk       INT64,
  eligible       BOOL,
  reason         STRING,
  valid_from     DATE,
  valid_to       DATE,
  load_date      DATE
)
PARTITION BY load_date
OPTIONS (
  description = 'Customer promo eligibility bridge. Source: retail.bridge_promo_eligibility (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
