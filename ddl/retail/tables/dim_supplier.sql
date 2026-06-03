-- BigQuery DDL for retail.dim_supplier
-- Source: retail.dim_supplier (Hive managed table, acme-analytics cluster)
-- Conversion: STRUCT<...> primary_contact preserved as-is
-- Conversion: ARRAY<STRING> categories preserved as-is
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_supplier` (
  supplier_sk        INT64,
  supplier_id        STRING,
  supplier_name      STRING,
  country            STRING,
  tax_id             STRING,
  payment_terms_days INT64,
  onboard_dt         DATE,
  risk_rating        STRING,
  primary_contact    STRUCT<name STRING, email STRING, phone STRING>,
  categories         ARRAY<STRING>
)
OPTIONS (
  description = 'Supplier/vendor master dimension. Source: retail.dim_supplier (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
