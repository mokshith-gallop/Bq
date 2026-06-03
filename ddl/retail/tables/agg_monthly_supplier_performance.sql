-- BigQuery DDL for retail.agg_monthly_supplier_performance
-- Source: retail.agg_monthly_supplier_performance (Hive managed table, acme-analytics cluster)
-- Partition: month_start DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_monthly_supplier_performance` (
  supplier_sk         INT64,
  orders_placed       INT64,
  units_received      INT64,
  on_time_pct         NUMERIC(5,4),
  fill_rate_pct       NUMERIC(5,4),
  avg_lead_time_days  NUMERIC(6,2),
  quality_score       NUMERIC(4,3),
  total_spend         NUMERIC(16,2),
  month_start         DATE
)
PARTITION BY month_start
OPTIONS (
  description = 'Monthly supplier performance metrics. Source: retail.agg_monthly_supplier_performance (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
