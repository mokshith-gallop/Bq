-- BigQuery DDL for retail.dim_category
-- Source: retail.dim_category (Hive managed table, acme-analytics cluster)
-- Hierarchical product category (self-referencing via parent_id)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_category` (
  category_sk     INT64,
  category_id     STRING,
  parent_id       STRING,
  name            STRING,
  depth           INT64,
  sort_order      INT64
)
OPTIONS (
  description = 'Hierarchical product category dimension (self-referencing). Source: retail.dim_category (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
