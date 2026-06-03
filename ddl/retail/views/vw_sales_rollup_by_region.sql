-- BigQuery view for retail.vw_sales_rollup_by_region
-- Source: retail.vw_sales_rollup_by_region (Hive view, acme-analytics cluster)
-- Dialect translation:
--   GROUPING__ID → GROUPING_ID(s.region, s.store_sk)
--   GROUP BY s.region, s.store_sk WITH ROLLUP → GROUP BY ROLLUP(s.region, s.store_sk)
--   date_sub(current_date(), 7) → DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_sales_rollup_by_region` AS
SELECT
    s.region,
    s.store_sk,
    SUM(f.line_total)              AS total_revenue,
    COUNT(*)                       AS line_count,
    GROUPING_ID(s.region, s.store_sk) AS grouping_level
FROM `${PROJECT_US}.${DS_RETAIL}.fact_sales` f
JOIN `${PROJECT_US}.${DS_RETAIL}.dim_store` s ON s.store_sk = f.customer_sk
WHERE f.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY ROLLUP(s.region, s.store_sk);
