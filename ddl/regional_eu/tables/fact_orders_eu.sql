-- BigQuery DDL for regional.fact_orders_eu
-- Source: regional.fact_orders_eu (Hive managed table, acme-edge cluster)
-- Conversion: multi-column partition (order_year/order_month INT, country_partition STRING) →
--   partition_month DATE via generated column + CLUSTER BY country_partition, customer_id
-- Conversion: CLUSTERED BY (customer_id) INTO 16 BUCKETS → included in CLUSTER BY

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.fact_orders_eu` (
  order_id        STRING,
  customer_id     STRING,
  sku             STRING,
  quantity        INT64,
  unit_price      NUMERIC(10,2),
  line_total      NUMERIC(14,2),
  currency_code   STRING,
  vat_amount      NUMERIC(12,2),
  order_ts        TIMESTAMP,
  country_iso2    STRING,
  order_year      INT64,
  order_month     INT64,
  country_partition STRING,
  partition_month DATE AS (DATE(order_year, order_month, 1))
)
PARTITION BY partition_month
CLUSTER BY country_partition, customer_id
OPTIONS (
  description = 'EU orders fact table. Source: regional.fact_orders_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
