# Type Mapping

## Hive to BigQuery Dialect Schema Type Mapping

The structural translation from Cloudera Hive to Google BigQuery will follow a strict, deterministic set of rules enforced during migration, schema generation, and in-memory Spark ETL loading:

### 1. Scalar Type Conversions

| Hive Dialect Type | BigQuery Target Type | Translation Rule & Notes |
|---|---|---|
| `TINYINT` | `INT64` | Widened to 64-bit integer (no native `INT8` in BQ) |
| `SMALLINT` | `INT64` | Widened to 64-bit integer (no native `INT16` in BQ) |
| `INT` | `INT64` | Widened to 64-bit integer (no native `INT32` in BQ) |
| `BIGINT` | `INT64` | Native 64-bit integer |
| `BOOLEAN` | `BOOL` | Boolean mapping |
| `FLOAT` | `FLOAT64` | Widened to 64-bit floating point |
| `DOUBLE` | `FLOAT64` | Native 64-bit floating point |
| `DECIMAL(p, s)` | `NUMERIC(p, s)` | Precision up to 38, scale up to 9. Fixed-point decimal mapping. |
| `STRING` | `STRING` | Variable-length character string |
| `VARCHAR` | `STRING` | Variable-length character string |
| `CHAR` | `STRING` | Variable-length character string |
| `TIMESTAMP` | `TIMESTAMP` | ISO 8601 representation (with microsecond precision, UTC base) |
| `DATE` | `DATE` | Standard Calendar Date representation |

### 2. Complex & Semi-Structured Type Conversions

#### Map Types
- **Hive `MAP<STRING, STRING>` → BigQuery `JSON`**
- Converts flexible map parameters (such as `properties` in `raw.mobile_events` or `utm` in `raw.email_campaign_clicks`) into native `JSON` columns.
- The ETL job `spark/bulk_load.py` uses Spark's `to_json` to serialize Maps, preserving nested structures, key order independence, and empty formats (`{}` or null).

#### Struct & Array Types
- **Hive `STRUCT<...>` → BigQuery `STRUCT<...>`**
- Hive Struct fields are mapped to BQ STRUCT elements with type widening recursively applied (e.g. `STRUCT<lat:DOUBLE, lon:DOUBLE>` maps directly).
- **Hive `ARRAY<STRUCT<...>>` → BigQuery `ARRAY<STRUCT<...>>`**
- Keeps nested transaction schemas (e.g. `line_items` in `raw.supplier_invoices`) completely intact.
- **Hive `ARRAY<STRING>` → BigQuery `ARRAY<STRING>`**
- Direct native support in BigQuery.

