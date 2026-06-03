-- BigQuery DDL for retail.inventory_realtime_snapshot
-- Source: retail.kudu_inventory_realtime (Kudu table, acme-analytics cluster)
-- Kudu batch analytics snapshot — per locked kudu_realtime_migration decision
-- Conversion: PRIMARY KEY dropped (BigQuery has no PK enforcement)
-- Conversion: PARTITION BY HASH dropped (no BQ equivalent)
-- Conversion: BIGINT last_updated_ts (epoch ms) → TIMESTAMP (convert via TIMESTAMP_MILLIS during load)
-- Conversion: STORED AS KUDU + TBLPROPERTIES → standard managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.inventory_realtime_snapshot` (
  warehouse_id    STRING,
  sku             STRING,
  on_hand         INT64,
  allocated       INT64,
  available       INT64,
  last_updated_ts TIMESTAMP
)
OPTIONS (
  description = 'Kudu inventory snapshot for batch analytics. Source: retail.kudu_inventory_realtime (Kudu, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
