-- BigQuery DDL for raw.chat_transcripts
-- Source: raw.chat_transcripts (Hive TSV external table, acme-lake cluster)
-- Conversion: date_ts STRING partition → partition_date DATE added; original date_ts preserved

CREATE TABLE IF NOT EXISTS `${PROJECT_US}.${DS_RAW}.chat_transcripts` (
  chat_id        STRING,
  customer_id    STRING,
  agent_id       STRING,
  started_at     TIMESTAMP,
  ended_at       TIMESTAMP,
  duration_sec   INT64,
  message_count  INT64,
  transcript     STRING,
  sentiment      NUMERIC(4,3),
  date_ts        STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS (
  description = 'Customer service chat transcripts. Source: raw.chat_transcripts (Hive, acme-lake cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'standard')]
);
