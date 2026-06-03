-- BigQuery view for raw.v_fraud_signals_recent
-- Source: raw.v_fraud_signals_recent (Hive view, acme-lake cluster)
-- Dialect translation:
--   date_format(date_sub(current_date(), 1), 'yyyyMMdd')
--     → FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RAW}.v_fraud_signals_recent` AS
SELECT *
FROM `${PROJECT_US}.${DS_RAW}.fraud_signals`
WHERE signal_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
