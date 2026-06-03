-- BigQuery DDL for retail.fact_shipments
-- Source: retail.fact_shipments (Hive managed table, acme-analytics cluster)
-- Conversion: multi-column partition (ship_year/ship_month/ship_day INT, carrier_partition STRING) →
--   partition_date DATE via generated column + CLUSTER BY carrier_partition
-- Conversion: CLUSTERED BY (warehouse_sk) INTO 16 BUCKETS → dropped (carrier_partition in CLUSTER BY per locked decision)
-- Conversion: ARRAY<STRUCT<...>> tracking_events preserved as-is

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.fact_shipments` (
  shipment_id     STRING,
  invoice_no      STRING,
  customer_sk     INT64,
  warehouse_sk    INT64,
  carrier         STRING,
  tracking_no     STRING,
  shipped_ts      TIMESTAMP,
  delivered_ts    TIMESTAMP,
  sla_hours       INT64,
  tracking_events ARRAY<STRUCT<ts TIMESTAMP, status STRING, location STRING>>,
  ship_year       INT64,
  ship_month      INT64,
  ship_day        INT64,
  carrier_partition STRING,
  partition_date  DATE AS (DATE(ship_year, ship_month, ship_day))
)
PARTITION BY partition_date
CLUSTER BY carrier_partition
OPTIONS (
  description = 'Outbound shipment events with carrier tracking. Source: retail.fact_shipments (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')],
  require_partition_filter = TRUE
);
