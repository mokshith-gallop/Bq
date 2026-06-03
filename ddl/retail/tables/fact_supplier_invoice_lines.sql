-- BigQuery DDL for retail.fact_supplier_invoice_lines
-- Source: retail.fact_supplier_invoice_lines (Hive managed table, acme-analytics cluster)
-- Conversion: multi-column partition (invoice_year/invoice_month INT) → partition_month DATE via generated column

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_supplier_invoice_lines` (
  invoice_line_id  INT64,
  invoice_no       STRING,
  supplier_sk      INT64,
  sku              STRING,
  quantity         INT64,
  unit_cost        NUMERIC(12,4),
  line_total       NUMERIC(14,2),
  currency_code    STRING,
  received_ts      TIMESTAMP,
  invoice_year     INT64,
  invoice_month    INT64,
  partition_month  DATE AS (DATE(invoice_year, invoice_month, 1))
)
PARTITION BY partition_month
OPTIONS (
  description = 'Line-level supplier invoicing. Source: retail.fact_supplier_invoice_lines (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
