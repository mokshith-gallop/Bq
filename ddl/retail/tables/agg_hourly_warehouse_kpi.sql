-- BigQuery DDL for retail.agg_hourly_warehouse_kpi
-- Source: retail.agg_hourly_warehouse_kpi (Hive managed table, acme-analytics cluster)
-- Partition: snapshot_hour STRING — no date conversion (hourly string key)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.agg_hourly_warehouse_kpi` (
  warehouse_sk        INT64,
  units_in            INT64,
  units_picked        INT64,
  units_shipped       INT64,
  pick_rate_uph       NUMERIC(8,2),
  backlog_units       INT64,
  avg_pick_seconds    NUMERIC(8,2),
  snapshot_hour       STRING
)
OPTIONS (
  description = 'Warehouse operational metrics (hourly). Source: retail.agg_hourly_warehouse_kpi (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
