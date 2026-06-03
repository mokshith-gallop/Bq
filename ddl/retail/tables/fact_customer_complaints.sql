-- BigQuery DDL for retail.fact_customer_complaints
-- Source: retail.fact_customer_complaints (Hive managed table, acme-analytics cluster)
-- Partition: created_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_customer_complaints` (
  complaint_id    STRING,
  customer_sk     INT64,
  invoice_no      STRING,
  channel         STRING,
  severity        STRING,
  summary         STRING,
  created_at      TIMESTAMP,
  resolved_at     TIMESTAMP,
  csat_score      INT64,
  created_date    DATE
)
PARTITION BY created_date
OPTIONS (
  description = 'Ticketed customer complaints fact. Source: retail.fact_customer_complaints (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
