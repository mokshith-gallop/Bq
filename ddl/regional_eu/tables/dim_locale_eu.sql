-- BigQuery DDL for regional.dim_locale_eu
-- Source: regional.dim_locale_eu (Hive managed table, acme-edge cluster)
-- No partition in source — plain managed table

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.dim_locale_eu` (
  locale_code     STRING,
  country_iso2    STRING,
  language_iso2   STRING,
  timezone        STRING,
  currency_code   STRING,
  decimal_sep     STRING,
  date_format     STRING
)
OPTIONS (
  description = 'Locale master for EU markets. Source: regional.dim_locale_eu (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
