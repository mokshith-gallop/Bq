-- BigQuery DDL for retail.bridge_customer_segment
-- Source: retail.bridge_customer_segment (Hive managed table, acme-analytics cluster)
-- Partition: snapshot_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.bridge_customer_segment` (
  customer_sk     INT64,
  segment_id      STRING,
  segment_name    STRING,
  assigned_dt     DATE,
  expires_dt      DATE,
  confidence      NUMERIC(4,3),
  source          STRING,
  snapshot_date   DATE
)
PARTITION BY snapshot_date
OPTIONS (
  description = 'Customer M:N segment memberships bridge. Source: retail.bridge_customer_segment (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
