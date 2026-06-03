-- BigQuery DDL for staging.dedup_clickstream
-- Source: staging.dedup_clickstream (Hive managed table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved
-- Conversion: country_partition STRING partition → CLUSTER BY
-- Conversion: CLUSTERED BY (user_id) INTO 16 BUCKETS → CLUSTER BY user_id (no INTO N BUCKETS)

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_STAGING}.dedup_clickstream` (
  session_id     STRING,
  user_id        STRING,
  event_ts       TIMESTAMP,
  page_url       STRING,
  referrer_url   STRING,
  ip             STRING,
  country        STRING,
  bot_score      NUMERIC(4,3),
  device_type    STRING,
  date_ts        STRING,
  country_partition STRING,
  partition_date DATE
)
PARTITION BY partition_date
CLUSTER BY country_partition, user_id
OPTIONS (
  description = 'Dedup''d clickstream — Omniture stream with bot/duplicate filtering. Source: staging.dedup_clickstream (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
