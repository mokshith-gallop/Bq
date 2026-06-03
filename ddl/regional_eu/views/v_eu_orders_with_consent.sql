-- BigQuery view for regional.v_eu_orders_with_consent
-- Source: regional.v_eu_orders_with_consent (Hive view, acme-edge cluster)
-- All refs → ${PROJECT_EU}.${DS_REGIONAL}
-- Dialect: clean SQL — no Hive-specific constructs

CREATE OR REPLACE VIEW `${PROJECT_EU}.${DS_REGIONAL}.v_eu_orders_with_consent` AS
SELECT
    o.order_id,
    o.customer_id,
    o.country_iso2,
    o.line_total,
    o.currency_code,
    COALESCE(c.granted, FALSE)            AS marketing_consent,
    c.granted_ts                          AS consent_granted_ts
FROM `${PROJECT_EU}.${DS_REGIONAL}.fact_orders_eu` o
LEFT JOIN `${PROJECT_EU}.${DS_REGIONAL}.dim_gdpr_consent` c
       ON c.customer_id = o.customer_id
      AND c.consent_type = 'MARKETING'
      AND c.granted = TRUE
      AND (c.withdrawn_ts IS NULL OR c.withdrawn_ts > o.order_ts);
