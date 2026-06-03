-- BigQuery DDL for retail.dim_geography
-- Source: retail.dim_geography (Hive managed table, acme-analytics cluster)
-- Non-partitioned dimension table

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RETAIL}.dim_geography` (
  geo_sk           INT64,
  country_iso2     STRING,
  country_name     STRING,
  region_code      STRING,
  region_name      STRING,
  city             STRING,
  postal_code      STRING,
  timezone         STRING,
  latitude         FLOAT64,
  longitude        FLOAT64
)
OPTIONS (
  description = 'Country/region/city geography hierarchy dimension. Source: retail.dim_geography (Hive, acme-analytics cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
