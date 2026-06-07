# Validation

The validation strategy establishes a automated, dual-layer post-deployment audit mechanism inside `/workspace/project/scripts/deploy_ddl.py` to confirm structural, dialect, and syntactic parity across the entire BigQuery topology.

### 1. Verification Layers

- **Layer 1: Structural Audit via INFORMATION_SCHEMA**:
  - Immediately following schema creation, the deployment script queries target metadata via `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`, and `INFORMATION_SCHEMA.PARTITIONS`.
  - Audits actual partition fields (e.g. `partition_date` exists and is formatted as native `DATE`), clustering configurations, and explicit types (e.g. verifying `MAP` columns resolved to native `JSON` and `DECIMAL` fields mapped to exact fixed-precision `NUMERIC` types).
  - Asserts that all 100 tables exist across the US (`raw`, `staging`, `retail`, `udfs`) and EU (`regional_eu`, `udfs`) datasets.

- **Layer 2: View Dialect & Compilation Dry-Run Verification**:
  - Executes dry-run query jobs for all 15 converted BigQuery views (e.g., `vw_panel_continuity_score`, `v_eu_orders_with_consent`, `vw_session_to_order_attribution`).
  - By using BigQuery's dry-run feature (`dry_run = True` on query jobs), the script tests syntactic validity, checks for translated function compatibility (e.g., `DATE_DIFF`, `APPROX_COUNT_DISTINCT`, `FORMAT_DATE`), and validates cross-dataset references without reading/billing scanned bytes.

### 2. Failure Handling
- If any table fails the `INFORMATION_SCHEMA` structure check, or if a view dry-run throws a compile-time SQL execution error (due to broken dependencies, invalid function calls, or dataset location conflicts), the script raises a verbose schema validation exception, lists the faulty schemas, and exits with a non-zero exit code to halt automated CI/CD pipeline triggers.
