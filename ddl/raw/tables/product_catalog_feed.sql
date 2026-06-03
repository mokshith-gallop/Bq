-- BigQuery DDL for raw.product_catalog_feed
-- Source: raw.product_catalog_feed (Hive RCFile external table, acme-lake cluster)
-- Conversion: MAP<STRING,STRING> metadata → JSON
-- Conversion: feed_date STRING partition → partition_date DATE added; original feed_date preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.product_catalog_feed` (
  sku            STRING,
  supplier_id    STRING,
  upc            STRING,
  name           STRING,
  category       STRING,
  subcategory    STRING,
  color          STRING,
  size           STRING,
  msrp           NUMERIC(10,2),
  cost           NUMERIC(10,2),
  available_from DATE,
  discontinued_at DATE,
  metadata       JSON,
  feed_date      STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Product catalog supplier feed (legacy RCFile). Source: raw.product_catalog_feed (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
