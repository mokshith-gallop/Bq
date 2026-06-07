# Validation Strategy

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

