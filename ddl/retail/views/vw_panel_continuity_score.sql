-- BigQuery view for retail.vw_panel_continuity_score
-- Source: retail.vw_panel_continuity_score (Hive view, acme-analytics cluster)
-- Dialect translation:
--   normalize_country(x) → `${PROJECT_US}.${DS_UDFS}.normalize_country`(x)
--   date_sub(current_date(), 90) → DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
-- Note: depends on UDF — must be deployed AFTER udfs.normalize_country is created

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_panel_continuity_score` AS
SELECT
    f.customer_sk,
    COUNT(DISTINCT f.sale_date)         AS active_days,
    COUNT(DISTINCT f.product_sk)        AS distinct_products,
    SUM(f.line_total)                   AS total_spend
FROM `${PROJECT_US}.${DS_RETAIL}.fact_sales` f
JOIN `${PROJECT_US}.${DS_RETAIL}.dim_customer` c
  ON c.customer_sk = f.customer_sk
 AND `${PROJECT_US}.${DS_UDFS}.normalize_country`(c.country) = `${PROJECT_US}.${DS_UDFS}.normalize_country`(f.country)
WHERE f.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY f.customer_sk;
