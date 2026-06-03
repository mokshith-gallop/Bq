-- BigQuery DDL for regional.fact_returns_eu
-- Source: regional.fact_returns_eu (Hive managed table, acme-edge cluster)
-- Partition: return_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.fact_returns_eu` (
  return_id       INT64,
  order_id        STRING,
  customer_id     STRING,
  sku             STRING,
  return_ts       TIMESTAMP,
  refund_amount   NUMERIC(12,2),
  reason_code     STRING,
  country_iso2    STRING,
  return_date     DATE
)
PARTITION BY return_date
OPTIONS (
  description = 'EU returns fact table. Source: regional.fact_returns_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
