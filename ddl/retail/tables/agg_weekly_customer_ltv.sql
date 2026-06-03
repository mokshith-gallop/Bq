-- BigQuery DDL for retail.agg_weekly_customer_ltv
-- Source: retail.agg_weekly_customer_ltv (Hive managed table, acme-analytics cluster)
-- Partition: week_start_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_weekly_customer_ltv` (
  customer_sk         INT64,
  ltv_to_date         NUMERIC(16,2),
  orders_to_date      INT64,
  avg_order_value     NUMERIC(12,2),
  days_since_last_order INT64,
  rfm_score           STRING,
  churn_risk          NUMERIC(4,3),
  week_start_date     DATE
)
PARTITION BY week_start_date
OPTIONS (
  description = 'Cumulative customer lifetime value (weekly). Source: retail.agg_weekly_customer_ltv (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
