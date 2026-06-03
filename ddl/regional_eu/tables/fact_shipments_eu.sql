-- BigQuery DDL for regional.fact_shipments_eu
-- Source: regional.fact_shipments_eu (Hive managed table, acme-edge cluster)
-- Partition: ship_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.fact_shipments_eu` (
  shipment_id     STRING,
  order_id        STRING,
  customer_id     STRING,
  carrier         STRING,
  tracking_no     STRING,
  shipped_at      TIMESTAMP,
  delivered_at    TIMESTAMP,
  country_from    STRING,
  country_to      STRING,
  sla_hours       INT64,
  ship_date       DATE
)
PARTITION BY ship_date
OPTIONS (
  description = 'EU shipments with carrier tracking. Source: regional.fact_shipments_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
