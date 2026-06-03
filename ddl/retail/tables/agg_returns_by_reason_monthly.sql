-- BigQuery DDL for retail.agg_returns_by_reason_monthly
-- Source: retail.agg_returns_by_reason_monthly (Hive managed table, acme-analytics cluster)
-- Partition: month_start DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_returns_by_reason_monthly` (
  reason_code         STRING,
  return_count        INT64,
  return_units        INT64,
  total_refunded      NUMERIC(16,2),
  avg_days_to_return  NUMERIC(8,2),
  month_start         DATE
)
PARTITION BY month_start
OPTIONS (
  description = 'Returns by reason code (monthly). Source: retail.agg_returns_by_reason_monthly (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
