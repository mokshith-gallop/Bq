-- BigQuery DDL for retail.fact_promo_redemptions
-- Source: retail.fact_promo_redemptions (Hive managed table, acme-analytics cluster)
-- Partition: redemption_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_promo_redemptions` (
  redemption_id   INT64,
  promo_sk        INT64,
  invoice_no      STRING,
  customer_sk     INT64,
  discount_amount NUMERIC(12,2),
  applied_ts      TIMESTAMP,
  channel         STRING,
  redemption_date DATE
)
PARTITION BY redemption_date
OPTIONS (
  description = 'Promotion redemption fact. Source: retail.fact_promo_redemptions (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
