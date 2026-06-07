# Locked Decisions for Story cfdf7f1f-b718-471f-b09b-925452749a7e

## Type Mapping
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


## Constraints & Indexes
## BigQuery Constraints & Metadata Strategy

Google BigQuery operates as an analytical database engine where traditional primary key/foreign key constraints are primary metadata descriptors for query optimization rather than strictly enforced constraints:

### 1. Primary Keys and Foreign Keys
- **Primary & Foreign Keys in Target BigQuery DDL**:
  - Validated by design to ensure relationships are accurately modeled.
  - Declared as `NOT ENFORCED` metadata relationships where applicable, providing hints to BI tools and query planners without write-path degradation or performance bottlenecks.
- **Data Quality & Relational Integrity Enforcement**:
  - Relational integrity (e.g. parent-child checks) will be validated by **Production DQ Validators** running in Cloud Composer DAGs (using Airflow `BigQueryCheckOperator` queries verifying anti-joins return exactly 0 rows).

### 2. Required Partitions & Clustering Constraints
- **Require Partition Filter**:
  - 11 high-risk transactional and event tables (including `fact_sales` and `mobile_events`) will have `require_partition_filter = TRUE` applied under `OPTIONS`.
  - Enforces queries against these high-volume tables to provide explicit partition pruning logic, completely preventing runaway scan costs.
- **Clustering**:
  - Replaces traditional indexing and bucket partition models (`CLUSTERED BY ... INTO N BUCKETS`).
  - Up to 4 columns will be assigned under `CLUSTER BY` to speed up join predicates, point lookups, and range filtering dynamically.


## Validation Strategy
## Live End-to-End Data Verification Strategy

To guarantee data correctness post-migration, we will execute a live, end-to-end data validation process comparing the source Hive databases directly with the target BigQuery datasets:

### 1. Multi-Layer Verification Pattern
The verification workflow operates at three complementary levels to guarantee absolute correctness and consistency:
- **Layer 1: Row Count Parity** — Verify that the row counts in BigQuery match the source Hive tables exactly (0 row difference) for all 82 tables across every partition.
- **Layer 2: Column-Level Aggregations** — Run parallel numeric aggregate comparisons (`SUM`, `MIN`, `MAX`, `AVG`) across primary numeric, decimal, and metric columns, as well as null counts (`COUNTIF(col IS NULL)`) and unique constraints (`COUNT(DISTINCT)`).
- **Layer 3: Row-Level Data Fingerprinting (Hashing)** — For all 30 **Critical** tables, run high-fidelity bitwise hashing (e.g. `BIT_XOR(SHA256(...))`) over sorted, serialized, and canonicalized row values to detect and isolate individual row anomalies, precision mismatches, or translation drift.

### 2. Live Verification Architecture (GCS-Mediated Parity Run)
Because Cloud Composer 2 cannot establish direct low-latency connections to on-premise JDBC/Hive interfaces during heavy batch runs, the validation uses a GCS-mediated runtime model:
1. **Source Capture Phase (Airflow Task Group)**: For each of the on-premise clusters (`acme-lake`, `acme-analytics`, `acme-edge`), a master bash script SSHs to the cluster and executes high-speed `beeline` queries to compute row counts and partition statistics bounded by the frozen watermark **W**.
2. **Metadata Upload**: The on-premise queries produce a structured, machine-readable `source_counts.json` manifest which is securely written to `gs://${GCS_STAGING_US}/manifests/source_counts.json`.
3. **Target BigQuery Audit**: Downstream Python tasks (running as `inline_validation` Airflow operators) parse this JSON, query BigQuery for actual counts, and assert perfect parity. Any row or partition count mismatch immediately halts downstream execution and triggers a Slack alert.


## UDF Deployment Strategy
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

