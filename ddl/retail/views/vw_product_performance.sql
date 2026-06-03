-- BigQuery view for retail.vw_product_performance
-- Source: retail.vw_product_performance (Hive view, acme-analytics cluster)
-- Dialect: clean SQL — only fully-qualify table references

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_product_performance` AS
WITH sold AS (
  SELECT
    p.product_sk,
    p.stock_code,
    p.description,
    f.country,
    SUM(f.line_total)                AS revenue,
    SUM(f.quantity)                  AS units,
    COUNT(DISTINCT f.invoice_no)     AS orders
  FROM `${PROJECT_US}.${DS_RETAIL}.dim_product` p
  JOIN `${PROJECT_US}.${DS_RETAIL}.fact_sales` f ON f.product_sk = p.product_sk
  GROUP BY p.product_sk, p.stock_code, p.description, f.country
),
ranked AS (
  SELECT
    s.*,
    RANK()       OVER (PARTITION BY s.country ORDER BY s.revenue DESC)  AS country_rank,
    DENSE_RANK() OVER (                       ORDER BY s.revenue DESC)  AS global_rank
  FROM sold s
)
SELECT * FROM ranked;
