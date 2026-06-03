-- BigQuery DDL for regional.dim_gdpr_consent
-- Source: regional.dim_gdpr_consent (Hive managed table, acme-edge cluster)
-- Partition: consent_date DATE (native DATE partition — no conversion needed)

CREATE TABLE IF NOT EXISTS `${PROJECT_EU}.${DS_REGIONAL}.dim_gdpr_consent` (
  consent_id      STRING,
  customer_id     STRING,
  consent_type    STRING,
  granted         BOOL,
  granted_ts      TIMESTAMP,
  withdrawn_ts    TIMESTAMP,
  source          STRING,
  legal_basis     STRING,
  expiry_ts       TIMESTAMP,
  consent_date    DATE
)
PARTITION BY consent_date
OPTIONS (
  description = 'GDPR consent tracking (compliance-critical). Source: regional.dim_gdpr_consent (Hive, acme-edge cluster).',
  labels = [('source_system', 'cloudera'), ('migration_tier', 'critical')]
);
