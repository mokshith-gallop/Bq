-- BigQuery DDL for staging.warehouse_kpi_snapshot
-- Source: staging.warehouse_kpi_snapshot (Hive managed table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.warehouse_kpi_snapshot` (
  warehouse_id   STRING,
  snapshot_ts    TIMESTAMP,
  units_in       INT64,
  units_picked   INT64,
  units_shipped  INT64,
  pick_rate_uph  NUMERIC(8,2),
  backlog_units  INT64,
  avg_pick_ms    INT64,
  date_ts        STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Warehouse KPI hourly snapshot. Source: staging.warehouse_kpi_snapshot (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
