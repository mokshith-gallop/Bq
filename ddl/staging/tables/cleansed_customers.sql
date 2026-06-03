-- BigQuery DDL for staging.cleansed_customers
-- Source: staging.cleansed_customers (Hive managed table, acme-lake cluster)
-- Partition: load_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.cleansed_customers` (
  customer_id     STRING,
  email_norm      STRING,
  phone_norm      STRING,
  first_name      STRING,
  last_name       STRING,
  addr_line1      STRING,
  addr_city       STRING,
  addr_region     STRING,
  addr_country    STRING,
  addr_postal     STRING,
  geocoded_lat    FLOAT64,
  geocoded_lon    FLOAT64,
  eff_from_ts     TIMESTAMP,
  record_hash     STRING,
  load_date       DATE
)
PARTITION BY load_date
OPTIONS (
  description = 'Cleansed customers — dedup''d customer records with SCD-2-ready effective dates. Source: staging.cleansed_customers (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
