-- BigQuery view for retail.vw_session_to_order_attribution
-- Source: retail.vw_session_to_order_attribution (Hive view, acme-analytics cluster)
-- Cross-dataset: raw.mobile_events → ${PROJECT_US}.${DS_RAW}.mobile_events
-- Dialect translation:
--   INTERVAL '1' DAY → INTERVAL 1 DAY
-- STRUCT field access s.context.referrer preserved as-is

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_RETAIL}.vw_session_to_order_attribution` AS
SELECT
  s.user_id,
  s.event_ts                                AS session_ts,
  s.context.referrer                        AS referrer,
  f.invoice_no,
  f.invoice_ts                              AS order_ts,
  f.line_total
FROM `${PROJECT_US}.${DS_RAW}.mobile_events` s
LEFT JOIN `${PROJECT_US}.${DS_RETAIL}.dim_customer` dc ON dc.customer_id = s.user_id
LEFT JOIN `${PROJECT_US}.${DS_RETAIL}.fact_sales` f
       ON  f.customer_sk = dc.customer_sk
       AND f.invoice_ts BETWEEN s.event_ts AND s.event_ts + INTERVAL 1 DAY;
