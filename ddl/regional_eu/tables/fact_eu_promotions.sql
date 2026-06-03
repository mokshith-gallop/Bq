-- BigQuery DDL for regional.fact_eu_promotions
-- Source: regional.fact_eu_promotions (Hive managed table, acme-edge cluster)
-- Partition: redemption_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.fact_eu_promotions` (
  redemption_id   INT64,
  promo_code      STRING,
  customer_id     STRING,
  order_id        STRING,
  discount_amount NUMERIC(12,2),
  currency_code   STRING,
  redeemed_ts     TIMESTAMP,
  country_iso2    STRING,
  redemption_date DATE
)
PARTITION BY redemption_date
OPTIONS (
  description = 'EU-specific promo redemption fact. Source: regional.fact_eu_promotions (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
