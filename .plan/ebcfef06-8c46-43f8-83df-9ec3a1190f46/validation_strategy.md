# Validation Strategy

## Inline Validation Strategy (Bulk Load Pipeline)

This decision covers the validation performed **within the bulk load pipeline itself** — the fast smoke tests that run immediately after each table loads. The full 4-layer validation (column aggregates, row-level fingerprints, BAQs) is a separate downstream pipeline defined by the locked Data Validation and Reconciliation decision.

### Inline Validation Scope: Row Count + Null-Key Check

After each Spark load task completes, a `BigQueryCheckOperator` task runs two checks:

**Check 1 — Row Count Parity**
```sql
-- Executed as a Composer BigQueryCheckOperator
SELECT
  CASE WHEN COUNT(*) = @expected_row_count THEN TRUE
       ELSE ERROR(FORMAT('Row count mismatch for %s.%s: expected %d, got %d',
                         @dataset, @table, @expected_row_count, COUNT(*)))
  END
FROM `@project.@dataset.@table`
```

The `expected_row_count` is captured pre-migration from Hive via:
```sql
-- Run on source cluster before DistCp, output stored in manifest
SET hive.local.time.zone=UTC;
SELECT COUNT(*) FROM ${db}.${table}
WHERE ${watermark_col} <= TIMESTAMP '${W}';
```

For partitioned tables, the check also validates partition count:
```sql
SELECT
  CASE WHEN COUNT(DISTINCT partition_date) = @expected_partition_count THEN TRUE
       ELSE ERROR(FORMAT('Partition count mismatch for %s.%s: expected %d, got %d',
                         @dataset, @table, @expected_partition_count,
                         COUNT(DISTINCT partition_date)))
  END
FROM `@project.@dataset.@table`
```

**Check 2 — Null Key Validation**
```sql
-- Validates that natural/primary key columns have no unexpected NULLs
SELECT
  CASE WHEN COUNTIF(@key_col IS NULL) = @expected_null_count THEN TRUE
       ELSE ERROR(FORMAT('Unexpected NULLs in %s.%s.%s: expected %d, got %d',
                         @dataset, @table, @key_col,
                         @expected_null_count, COUNTIF(@key_col IS NULL)))
  END
FROM `@project.@dataset.@table`
```

Key columns per table are defined in the YAML manifest under `validation.null_check_cols`. Examples:
- `fact_sales`: `[invoice_no, customer_sk, product_sk]`
- `returns_ledger`: `[return_id, invoice_no]`
- `dim_gdpr_consent`: `[consent_id, customer_id, granted]` (BOOLEAN — must not be NULL per GDPR)
- `pos_transactions`: `[txn_id, invoice_no]`

### Pre-Migration Row Count Capture

Before the DistCp phase begins, a preparatory step captures source row counts from all 82 Hive tables:

1. A Composer task SSHs to each cluster (or submits Beeline queries via `BashOperator`)
2. Runs `SELECT COUNT(*) FROM ${db}.${table} WHERE ${watermark_col} <= '${W}'` for each table
3. Results are written to a JSON manifest: `gs://acme-migration-staging-us/manifests/source_counts.json`
4. The inline validation tasks read expected counts from this manifest

For tables without a watermark column (static dimensions like `dim_date`, `dim_currency_eu`): full table `COUNT(*)` with no time bound.

### Frozen Watermark Recording

Per acceptance criterion 6, the pipeline records the watermark W at completion:

```python
# Final DAG task: record_watermark
record_watermark = BigQueryInsertJobOperator(
    task_id='record_watermark',
    configuration={
        'query': {
            'query': """
                CREATE OR REPLACE TABLE `acme-analytics.raw._migration_metadata` AS
                SELECT
                    CURRENT_TIMESTAMP() AS migration_completed_at,
                    TIMESTAMP '${W}' AS frozen_watermark_w,
                    82 AS total_tables_loaded,
                    'bulk_migration_dag' AS dag_id,
                    '${dag_run_id}' AS dag_run_id
            """,
            'useLegacySql': False,
        }
    },
)
```

The watermark W is agreed upon **before** the pipeline runs (per the locked Data Validation decision, section 3). It is passed as an Airflow variable and used consistently across:
- Source row count capture queries (Hive side)
- DistCp scope (only partitions up to W)
- Inline validation queries (BigQuery side)
- Downstream formal validation pipeline (layers 1-4)

### Failure Handling

| Scenario | Behavior |
|---|---|
| Row count mismatch on a STANDARD table | Task fails, logged, Slack alert. Other tables in the wave continue. Table is flagged for re-run. |
| Row count mismatch on a CRITICAL table | Task fails, logged, Slack alert. Other tables continue. **Blocks formal validation sign-off** until resolved. |
| Null key check fails | Task fails, logged. Likely indicates a data corruption or format parsing issue. Requires investigation before re-run. |
| Spark job fails (OOM, Storage Write API error) | Retried 3x with exponential backoff. If all retries fail, task is marked failed and Slack alert fires. |
| DistCp fails | Retried 2x. If persistent, likely a network or permission issue — requires manual investigation. |

### Handoff to Formal Validation

Once the bulk load pipeline completes:
1. All 82 tables are loaded in BigQuery with data bounded by watermark W
2. `_migration_metadata` table records W and completion timestamp
3. The formal validation pipeline (separate Composer DAG, per locked Data Validation decision) is triggered
4. Formal validation runs layers 1-4: row counts per partition, column aggregates, row-level fingerprints (30 CRITICAL tables), and BAQ-1 through BAQ-5

The inline validation in this pipeline is intentionally lightweight — it catches gross failures fast (empty tables, truncated loads, corrupt parsing) so that the formal validation pipeline doesn't waste hours discovering a table that loaded zero rows.

### Validation Summary

| Check | Scope | Threshold | When |
|---|---|---|---|
| Row count parity | All 82 tables | Exact match (= 0 delta) | After each table load |
| Partition count parity | All partitioned tables | Exact match | After each table load |
| Null key check | All 82 tables (key columns from manifest) | NULL count matches expected | After each table load |
| Watermark recording | Pipeline-level | W recorded and immutable | After all tables load |
| Source count capture | All 82 tables (Hive side) | Successfully captured before DistCp | Before pipeline starts |
