-- BigQuery DDL for com.acme.udf.LookupSupplierTerms
-- Remote UDF: lookup_supplier_terms(supplier_id STRING)
-- Materialized inside the 'udfs' dataset.
--
-- Location-agnostic template (using PROJECT_ID, DATASET, CONNECTION_ID, and FUNCTION_ENDPOINT)

CREATE OR REPLACE FUNCTION `${PROJECT_ID}.${DATASET}.lookup_supplier_terms`(supplier_id STRING)
RETURNS INT64
REMOTE WITH CONNECTION `${PROJECT_ID}.${CONNECTION_LOCATION}.${CONNECTION_ID}`
OPTIONS (
  endpoint = '${FUNCTION_ENDPOINT}'
);
