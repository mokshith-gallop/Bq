-- BigQuery DDL for retail.acid_supplier_terms_history
-- Source: retail.acid_supplier_terms_history (Hive ACID/ORC table, acme-analytics cluster)
-- Conversion: transactional=true → standard BigQuery managed table (BQ supports native UPDATE/DELETE/MERGE)
-- Conversion: CLUSTERED BY (supplier_sk) INTO 4 BUCKETS → CLUSTER BY supplier_sk
-- No PARTITION BY (non-partitioned ACID table in source)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.acid_supplier_terms_history` (
  history_id         INT64,
  supplier_sk        INT64,
  payment_terms_days INT64,
  discount_pct       NUMERIC(5,2),
  eff_from           TIMESTAMP,
  eff_to             TIMESTAMP,
  is_current         BOOL,
  changed_by         STRING
)
CLUSTER BY supplier_sk
OPTIONS (
  description = 'SCD-2 supplier payment-terms history (ACID source — standard BQ managed table). Source: retail.acid_supplier_terms_history (Hive ACID, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
