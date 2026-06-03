-- BigQuery DDL for raw.customer_signups
-- Source: raw.customer_signups (Hive Avro external table, acme-lake cluster)
-- Schema derived from customer_signups-v3.avsc
-- Conversion: signup_date STRING partition → partition_date DATE added; original signup_date preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.customer_signups` (
  customer_id     STRING,
  email           STRING,
  phone           STRING,
  first_name      STRING,
  last_name       STRING,
  addr_line1      STRING,
  addr_city       STRING,
  addr_region     STRING,
  addr_country    STRING,
  addr_postal     STRING,
  signup_source   STRING,
  marketing_opt_in BOOL,
  signup_date     STRING,
  partition_date  DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Customer signup events from web tier (Avro, schema-evolved). Source: raw.customer_signups (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
