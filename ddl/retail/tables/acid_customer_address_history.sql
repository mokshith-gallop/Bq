-- BigQuery DDL for retail.acid_customer_address_history
-- Source: retail.acid_customer_address_history (Hive ACID/ORC table, acme-analytics cluster)
-- Conversion: transactional=true → standard BigQuery managed table (BQ supports native UPDATE/DELETE/MERGE)
-- Conversion: CLUSTERED BY (customer_sk) INTO 8 BUCKETS → CLUSTER BY customer_sk
-- No PARTITION BY (non-partitioned ACID table in source)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.acid_customer_address_history` (
  history_id      INT64,
  customer_sk     INT64,
  address_line1   STRING,
  address_city    STRING,
  address_region  STRING,
  address_country STRING,
  address_postal  STRING,
  eff_from        TIMESTAMP,
  eff_to          TIMESTAMP,
  is_current      BOOL,
  change_reason   STRING
)
CLUSTER BY customer_sk
OPTIONS (
  description = 'SCD-2 customer address history (ACID source — standard BQ managed table). Source: retail.acid_customer_address_history (Hive ACID, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
