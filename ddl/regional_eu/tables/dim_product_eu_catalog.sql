-- BigQuery DDL for regional.dim_product_eu_catalog
-- Source: regional.dim_product_eu_catalog (Hive managed table, acme-edge cluster)
-- Conversion: ARRAY<STRING> eu_compliance_flags preserved as-is
-- No partition — plain managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.dim_product_eu_catalog` (
  sku                 STRING,
  eu_sku              STRING,
  name                STRING,
  description_de      STRING,
  description_fr      STRING,
  description_it      STRING,
  description_es      STRING,
  vat_pct             NUMERIC(5,4),
  eu_compliance_flags ARRAY<STRING>
)
OPTIONS (
  description = 'EU-specific product catalog with VAT and compliance flags. Source: regional.dim_product_eu_catalog (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
