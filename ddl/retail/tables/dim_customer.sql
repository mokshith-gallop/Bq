-- BigQuery DDL for retail.dim_customer
-- Source: retail.dim_customer (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_customer` (
  customer_sk    INT64,
  customer_id    STRING,
  country        STRING,
  first_seen_ts  TIMESTAMP,
  last_seen_ts   TIMESTAMP
)
OPTIONS (
  description = 'Customer dimension. Source: retail.dim_customer (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')]
);
