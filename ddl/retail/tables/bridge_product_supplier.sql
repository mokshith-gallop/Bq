-- BigQuery DDL for retail.bridge_product_supplier
-- Source: retail.bridge_product_supplier (Hive managed table, acme-analytics cluster)
-- No partition (non-partitioned in Hive)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.bridge_product_supplier` (
  product_sk         INT64,
  supplier_sk        INT64,
  primary_supplier   BOOL,
  supplier_sku       STRING,
  unit_cost          NUMERIC(12,4),
  lead_time_days     INT64,
  moq                INT64,
  valid_from         DATE,
  valid_to           DATE
)
OPTIONS (
  description = 'Product M:N supplier linkage. Source: retail.bridge_product_supplier (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
