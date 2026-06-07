# Constraints & Indexes

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

