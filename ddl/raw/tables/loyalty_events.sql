-- BigQuery DDL for raw.loyalty_events
-- Source: raw.loyalty_events (Hive RegexSerDe external table, acme-lake cluster)
-- Note: meta_raw is a STRING column (not MAP) — parsed via str_to_map() at query time.
--       In BigQuery, parse_key_value_pairs JS UDF handles conversion in views/queries.
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.loyalty_events` (
  event_ts_str   STRING,
  member_id      STRING,
  event_type     STRING,
  points         STRING,
  store_id       STRING,
  tx_id          STRING,
  meta_raw       STRING,
  date_ts        STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Loyalty events from older POS firmware (RegexSerDe parsed). Source: raw.loyalty_events (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
