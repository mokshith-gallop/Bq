-- BigQuery DDL for com.acme.udf.GeoIpCity
-- Remote UDF: geoip_city(ip STRING)
-- Materialized inside the 'udfs' dataset.
--
-- Location-agnostic template (using PROJECT_ID, DATASET, CONNECTION_ID, and FUNCTION_ENDPOINT)

CREATE OR REPLACE FUNCTION `${PROJECT_ID}.${DATASET}.geoip_city`(ip STRING)
RETURNS STRING
REMOTE WITH CONNECTION `${PROJECT_ID}.${CONNECTION_LOCATION}.${CONNECTION_ID}`
OPTIONS (
  endpoint = '${FUNCTION_ENDPOINT}'
);
