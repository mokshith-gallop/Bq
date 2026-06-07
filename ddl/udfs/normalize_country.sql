-- BigQuery DDL for com.acme.udf.NormalizeCountry
-- Native JavaScript UDF: normalize_country(country STRING)
-- Materialized inside the 'udfs' dataset.
--
-- Location-agnostic template (using PROJECT_ID and DATASET)

CREATE OR REPLACE FUNCTION `${PROJECT_ID}.${DATASET}.normalize_country`(country STRING)
RETURNS STRING
DETERMINISTIC
LANGUAGE js
AS r"""
  if (country === null || country === undefined) return null;
  var s = country.trim().toLowerCase();
  switch (s) {
    case "usa": case "us": case "united states":
    case "united states of america": case "u.s.a.":
      return "US";
    case "uk": case "gb": case "united kingdom": case "great britain":
      return "GB";
    case "de": case "germany": case "deutschland":
      return "DE";
    case "fr": case "france":
      return "FR";
    case "it": case "italy": case "italia":
      return "IT";
    case "ca": case "canada":
      return "CA";
    default:
      if (s.length === 2) return s.toUpperCase();
      return "XX";
  }
""";
