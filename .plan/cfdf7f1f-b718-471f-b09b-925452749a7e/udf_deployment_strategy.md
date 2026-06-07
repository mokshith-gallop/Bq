# UDF Deployment Strategy

## Three-Tier UDF Migration and Deployment Blueprint

To resolve custom Hive Java UDFs and guarantee dependent analytical views can compile and run correctly, we will implement a multi-tier deployment topology spanning native JavaScript UDFs and Cloud Functions-backed Remote UDFs.

### 1. UDF Mapping Topology

| Hive UDF Class | Target Type | BigQuery Target Registration (dataset: `udfs`) | Implementation Pattern |
|---|---|---|---|
| `com.acme.udf.NormalizeCountry` | JS UDF | `normalize_country(country STRING)` | Fast inline JS switch/map of country names to ISO-2 codes |
| `com.acme.udf.HashCustomerEmail` | JS UDF | `hash_customer_email(email STRING)` | JS-based SHA-256 hash (or native built-in optimization) |
| `com.acme.udf.ParseLegacySku` | JS UDF | `parse_legacy_sku(sku STRING)` | JS regex execution matching pre-2018 SKUs |
| `com.acme.udf.LookupSupplierTerms` | Remote | `lookup_supplier_terms(supplier_id STRING)` | Cloud Function reading `supplier_terms.csv` from GCS |
| `com.acme.udf.GeoIpCity` | Remote | `geoip_city(ip STRING)` | Cloud Function with embedded GeoLite2 DB resolver |

### 2. Implementation & Deployment Order (Strict Dependency Graph)
To prevent compile-time errors in views (such as `vw_panel_continuity_score` which depend directly on `normalize_country`), deployment must run in a strict sequence:
1. **Cloud Functions Deployment**: Deploy Cloud Functions for the 2 Remote UDFs (backed by Python runtimes) using GCS/Secret Manager access.
2. **BigQuery Remote Connections**: Provision the Remote Connections in US (`us-central1` or `US` multiregion connection) and EU (`europe-west3` or `EU` multiregion connection).
3. **IAM Grants**: Bind the Remote Connection service account to have invocation permissions (`roles/cloudfunctions.invoker`) on the deployed Cloud Functions.
4. **Register BigQuery UDFs**: Create the 5 UDF SQL signatures inside the `udfs` dataset of both US and EU projects (`CREATE OR REPLACE FUNCTION ...`).
5. **Compile Dependent Views**: Run DDL schemas for views, utilizing fully qualified names (e.g., `` `${PROJECT_US}.${DS_UDFS}.normalize_country`(c.country) ``).

