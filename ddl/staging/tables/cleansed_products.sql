-- BigQuery DDL for staging.cleansed_products
-- Source: staging.cleansed_products (Hive managed table, acme-lake cluster)
-- Partition: load_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.cleansed_products` (
  sku            STRING,
  upc            STRING,
  name_norm      STRING,
  category_norm  STRING,
  subcategory    STRING,
  color_norm     STRING,
  size_norm      STRING,
  msrp           NUMERIC(10,2),
  cost           NUMERIC(10,2),
  supplier_id    STRING,
  available      BOOL,
  load_date      DATE
)
PARTITION BY load_date
OPTIONS (
  description = 'Cleansed products — normalized SKU master. Source: staging.cleansed_products (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
