-- BigQuery DDL for raw.sales_retail
-- Source: raw.sales_retail (Hive external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.sales_retail` (
  invoice_no     STRING,
  stock_code     STRING,
  description    STRING,
  quantity       INT64,
  invoice_date   STRING,
  unit_price     NUMERIC(10,2),
  customer_id    STRING,
  country        STRING,
  date_ts        STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Raw retail sales landing table. Source: raw.sales_retail (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
