-- BigQuery view for staging.v_returns_pending
-- Source: staging.v_returns_pending (Hive view, acme-lake cluster)
-- Cross-dataset reference: raw.return_authorizations
-- Dialect translation:
--   DATEDIFF(current_date(), to_date(r.requested_at))
--     → DATE_DIFF(CURRENT_DATE(), DATE(r.requested_at), DAY)

CREATE OR REPLACE VIEW `${PROJECT_US}.${DS_STAGING}.v_returns_pending` AS
SELECT
    r.rma_id,
    r.customer_id,
    r.invoice_no,
    r.stock_code,
    r.quantity,
    r.requested_at,
    DATE_DIFF(CURRENT_DATE(), DATE(r.requested_at), DAY) AS days_pending
FROM `${PROJECT_US}.${DS_RAW}.return_authorizations` r
WHERE r.approved IS NULL OR r.approved = FALSE;
