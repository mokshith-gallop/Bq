-- BigQuery DDL for retail.dim_promotion
-- Source: retail.dim_promotion (Hive managed table, acme-analytics cluster)
-- Conversion: MAP<STRING,STRING> eligibility → JSON
-- Conversion: ARRAY<STRING> channels preserved as-is
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_promotion` (
  promo_sk        INT64,
  promo_id        STRING,
  name            STRING,
  promo_type      STRING,
  pct_off         NUMERIC(5,2),
  flat_off        NUMERIC(10,2),
  start_dt        DATE,
  end_dt          DATE,
  budget          NUMERIC(14,2),
  channels        ARRAY<STRING>,
  eligibility     JSON
)
OPTIONS (
  description = 'Promotion/marketing offers dimension. Source: retail.dim_promotion (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
