-- BigQuery view for retail.vw_otd_by_carrier_30d
-- Source: retail.vw_otd_by_carrier_30d (Hive view, acme-analytics cluster)
-- Dialect translation:
--   INTERVAL '48' HOUR → INTERVAL 48 HOUR
--   unix_timestamp(ts) → UNIX_SECONDS(ts)
--   date_sub(current_date(), 30) → DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_otd_by_carrier_30d` AS
SELECT
    carrier,
    COUNT(*)                                                          AS shipments,
    AVG(CASE WHEN delivered_ts <= shipped_ts + INTERVAL 48 HOUR
             THEN 1.0 ELSE 0.0 END)                                   AS otd_rate,
    AVG(UNIX_SECONDS(delivered_ts) - UNIX_SECONDS(shipped_ts)) / 3600.0 AS avg_transit_hours
FROM `${PROJECT_US}.${DS_RETAIL}.fact_shipments`
WHERE shipped_ts >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS TIMESTAMP)
GROUP BY carrier;
