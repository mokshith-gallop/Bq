-- BigQuery DDL for raw.fraud_signals
-- Source: raw.fraud_signals (Hive Avro external table, acme-lake cluster)
-- Schema derived from fraud_signals-v5.avsc
-- Conversion: signal_date STRING partition → partition_date DATE added; original signal_date preserved
-- Note: reason_codes is ARRAY<STRING> from Avro schema, preserved as-is
-- Note: signal_ts is logicalType timestamp-millis in Avro → TIMESTAMP in BigQuery

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.fraud_signals` (
  customer_id    STRING,
  signal_type    STRING,
  score          FLOAT64,
  risk_band      STRING,
  reason_codes   ARRAY<STRING>,
  signal_ts      TIMESTAMP,
  vendor         STRING,
  signal_date    STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Fraud detection signals (Avro, schema evolves quarterly). Source: raw.fraud_signals (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
