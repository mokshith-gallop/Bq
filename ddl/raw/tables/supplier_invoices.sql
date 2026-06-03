-- BigQuery DDL for raw.supplier_invoices
-- Source: raw.supplier_invoices (Hive SequenceFile external table, acme-lake cluster)
-- Conversion: multi-column partition (feed_year/feed_month INT) → partition_month DATE via generated column
-- Conversion: ARRAY<STRUCT<...>> line_items preserved as-is
-- Original INT partition columns preserved as regular columns

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.supplier_invoices` (
  invoice_no     STRING,
  supplier_id    STRING,
  invoice_date   DATE,
  due_date       DATE,
  total_amount   NUMERIC(14,2),
  currency       STRING,
  line_items     ARRAY<STRUCT<sku STRING, qty INT64, unit_price NUMERIC(10,2)>>,
  raw_xml        STRING,
  feed_year      INT64,
  feed_month     INT64,
  partition_month DATE AS (DATE(feed_year, feed_month, 1))
)
PARTITION BY partition_month
OPTIONS (
  description = 'Supplier invoices (legacy SequenceFile). Source: raw.supplier_invoices (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
