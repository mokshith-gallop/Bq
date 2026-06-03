-- BigQuery DDL for retail.agg_daily_carrier_otd
-- Source: retail.agg_daily_carrier_otd (Hive managed table, acme-analytics cluster)
-- Partition: ship_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_daily_carrier_otd` (
  carrier             STRING,
  shipments_total     INT64,
  delivered_on_time   INT64,
  delivered_late      INT64,
  in_transit          INT64,
  otd_pct             NUMERIC(5,4),
  avg_transit_hours   NUMERIC(8,2),
  ship_date           DATE
)
PARTITION BY ship_date
OPTIONS (
  description = 'On-time-delivery rate per carrier (daily). Source: retail.agg_daily_carrier_otd (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
