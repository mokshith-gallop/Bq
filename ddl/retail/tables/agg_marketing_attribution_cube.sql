-- BigQuery DDL for retail.agg_marketing_attribution_cube
-- Source: retail.agg_marketing_attribution_cube (Hive managed table, acme-analytics cluster)
-- Partition: period_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_marketing_attribution_cube` (
  channel             STRING,
  campaign_sk         INT64,
  region              STRING,
  attributed_revenue  NUMERIC(16,2),
  attributed_units    INT64,
  cost                NUMERIC(14,2),
  roas                NUMERIC(8,4),
  grouping_id         INT64,
  period_date         DATE
)
PARTITION BY period_date
OPTIONS (
  description = 'Pre-aggregated marketing attribution CUBE for BI tools. Source: retail.agg_marketing_attribution_cube (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
