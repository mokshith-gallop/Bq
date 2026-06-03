-- BigQuery view for raw.omniture
-- Source: raw.omniture (Hive view, acme-lake cluster)
-- Simple projection of typed columns from omniture_logs
-- Dialect: clean SQL — only fully-qualify table reference

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RAW}.omniture` AS
SELECT
    col_2  AS event_ts,
    col_8  AS ip,
    col_13 AS url,
    col_14 AS user_id,
    col_50 AS city,
    col_51 AS country,
    col_53 AS state,
    date_ts
FROM `${PROJECT_US}.${DS_RAW}.omniture_logs`;
