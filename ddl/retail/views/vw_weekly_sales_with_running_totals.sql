-- BigQuery view for retail.vw_weekly_sales_with_running_totals
-- Source: retail.vw_weekly_sales_with_running_totals (Hive view, acme-analytics cluster)
-- Dialect: clean SQL — only fully-qualify table references

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_weekly_sales_with_running_totals` AS
WITH weekly AS (
  SELECT
    d.d_week_seq,
    d.d_year,
    SUM(f.line_total) AS wk_revenue,
    SUM(f.quantity)   AS wk_units
  FROM `${PROJECT_US}.${DS_RETAIL}.fact_sales` f
  JOIN `${PROJECT_US}.${DS_RETAIL}.dim_date` d ON d.d_date = f.sale_date
  GROUP BY d.d_week_seq, d.d_year
)
SELECT
  w.d_week_seq,
  w.d_year,
  w.wk_revenue,
  w.wk_units,
  SUM(w.wk_revenue) OVER (
    PARTITION BY w.d_year
    ORDER BY w.d_week_seq
  )                                                              AS ytd_revenue,
  AVG(w.wk_revenue) OVER (
    ORDER BY w.d_week_seq
    ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
  )                                                              AS rolling_13wk_avg,
  LAG(w.wk_revenue, 52) OVER (ORDER BY w.d_week_seq)            AS same_week_last_year,
  w.wk_revenue - COALESCE(LAG(w.wk_revenue, 52) OVER (ORDER BY w.d_week_seq), 0)
                                                                 AS yoy_delta
FROM weekly w;
