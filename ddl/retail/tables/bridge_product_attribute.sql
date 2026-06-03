-- BigQuery DDL for retail.bridge_product_attribute
-- Source: retail.bridge_product_attribute (Hive managed table, acme-analytics cluster)
-- No partition (non-partitioned in Hive)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.bridge_product_attribute` (
  product_sk     INT64,
  attribute_name STRING,
  attribute_value STRING,
  primary_value  BOOL,
  sort_order     INT64
)
OPTIONS (
  description = 'Product M:N attribute values. Source: retail.bridge_product_attribute (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
