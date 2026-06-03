-- BigQuery DDL for raw.shipment_tracking
-- Source: raw.shipment_tracking (Hive CSV external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved
-- Conversion: carrier_partition STRING partition → CLUSTER BY

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.shipment_tracking` (
  tracking_no   STRING,
  carrier       STRING,
  invoice_no    STRING,
  customer_id   STRING,
  shipped_at    TIMESTAMP,
  delivered_at  TIMESTAMP,
  status        STRING,
  last_location STRING,
  estimated_eta TIMESTAMP,
  date_ts       STRING,
  carrier_partition STRING,
  partition_date DATE
)
PARTITION BY partition_date
CLUSTER BY carrier_partition
OPTIONS (
  description = 'Carrier shipment tracking (CSV from UPS/FedEx/DHL feeds). Source: raw.shipment_tracking (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
