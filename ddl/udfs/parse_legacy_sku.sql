-- BigQuery DDL for com.acme.udf.ParseLegacySku
-- Native JavaScript UDF: parse_legacy_sku(sku STRING)
-- Materialized inside the 'udfs' dataset.
--
-- Location-agnostic template (using PROJECT_ID and DATASET)

CREATE OR REPLACE FUNCTION `${PROJECT_ID}.${DATASET}.parse_legacy_sku`(sku STRING)
RETURNS STRING
DETERMINISTIC
LANGUAGE js
AS r"""
  if (sku === null || sku === undefined) return null;
  var s = sku.trim();
  var regex = /^([A-Z]{2})-(\d{3,5})-(\d{1,3})$/;
  var match = s.match(regex);
  if (!match) return "UNKNOWN|0|0";
  return match[1] + "|" + match[2] + "|" + match[3];
""";
