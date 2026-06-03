-- BigQuery DDL for retail.fact_sales
-- Source: retail.fact_sales (Hive managed table, acme-analytics cluster)
-- Conversion: CLUSTERED BY (customer_sk) INTO 8 BUCKETS → CLUSTER BY customer_sk
-- Partition: sale_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_sales` (
  invoice_no     STRING,
  customer_sk    INT64,
  product_sk     INT64,
  quantity       INT64,
  unit_price     NUMERIC(10,2),
  line_total     NUMERIC(14,2),
  country        STRING,
  invoice_ts     TIMESTAMP,
  sale_date      DATE
)
PARTITION BY sale_date
CLUSTER BY customer_sk
OPTIONS (
  description = 'Core revenue fact table (line-item sales). Source: retail.fact_sales (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
