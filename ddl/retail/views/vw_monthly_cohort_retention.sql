-- BigQuery view for retail.vw_monthly_cohort_retention
-- Source: retail.vw_monthly_cohort_retention (Hive view, acme-analytics cluster)
-- Dialect translation:
--   DATE_FORMAT(d, 'yyyy-MM') → FORMAT_DATE('%Y-%m', d)
--   MONTHS_BETWEEN(a, b)      → DATE_DIFF(a, b, MONTH)
--   to_date(concat(fo.cohort_month, '-01')) → PARSE_DATE('%Y-%m-%d', CONCAT(fo.cohort_month, '-01'))

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_monthly_cohort_retention` AS
WITH first_order AS (
  SELECT
    customer_sk,
    FORMAT_DATE('%Y-%m', MIN(sale_date)) AS cohort_month
  FROM `${PROJECT_US}.${DS_RETAIL}.fact_sales`
  GROUP BY customer_sk
),
orders AS (
  SELECT
    f.customer_sk,
    fo.cohort_month,
    FORMAT_DATE('%Y-%m', f.sale_date)                  AS order_month,
    DATE_DIFF(f.sale_date, PARSE_DATE('%Y-%m-%d', CONCAT(fo.cohort_month, '-01')), MONTH) AS months_since_first
  FROM `${PROJECT_US}.${DS_RETAIL}.fact_sales` f
  JOIN first_order fo ON fo.customer_sk = f.customer_sk
)
SELECT
  cohort_month,
  CAST(months_since_first AS INT64)    AS months_since_first,
  COUNT(DISTINCT customer_sk)          AS active_customers
FROM orders
GROUP BY cohort_month, CAST(months_since_first AS INT64);
