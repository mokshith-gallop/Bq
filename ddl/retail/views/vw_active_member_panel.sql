-- BigQuery view for retail.vw_active_member_panel
-- Source: retail.vw_active_member_panel (Hive/Impala view, acme-analytics cluster)
-- Dialect translation:
--   NDV(member_id) → APPROX_COUNT_DISTINCT(member_id)
--   date_sub(current_date(), 30) → DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_active_member_panel` AS
SELECT
    region,
    APPROX_COUNT_DISTINCT(member_id)   AS approx_active_members,
    COUNT(DISTINCT member_id)          AS exact_active_members,
    SUM(points)                        AS total_points_redeemed
FROM `${PROJECT_US}.${DS_RETAIL}.fact_loyalty_events`
WHERE event_type = 'REDEEM'
  AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY region;
