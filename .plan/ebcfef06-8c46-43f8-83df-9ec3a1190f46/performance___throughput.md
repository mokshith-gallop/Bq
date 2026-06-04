# Performance & Throughput

## Performance and Throughput Strategy

### Tiered Parallelism Model

Tables are grouped into 3 waves (US) + 1 wave (EU) based on estimated data volume, with different concurrency limits per wave:

| Wave | Tables | Concurrency | Dataproc Config | Rationale |
|---|---|---|---|---|
| **Wave 1 — Small** | ~35 tables: all `dim_*`, `bridge_*`, `agg_*`, `top_countries_daily`, `sales_cube`, small reference tables | 10-15 concurrent Dataproc Serverless jobs | 2 workers, `n2-standard-4` equivalent | Small tables (< 1 GB) — dominated by job startup overhead. High concurrency maximizes throughput. |
| **Wave 2 — Medium** | ~30 tables: all `staging.*`, medium fact tables (`fact_web_session`, `fact_returns`, `fact_warehouse_picks`, `fact_chat_interactions`, etc.) | 5-8 concurrent | 4 workers, `n2-standard-8` equivalent | Mid-range tables (1-50 GB). Moderate concurrency balances throughput vs. Dataproc quota. |
| **Wave 3 — Large** | ~17 tables: `fact_sales`, `omniture_logs`, `pos_transactions`, `mobile_events`, `fact_inventory_movements`, `fact_payments`, `fact_shipments`, 5 ACID tables | 3-4 concurrent | 8-16 workers, `n2-standard-16` equivalent, autoscaling enabled | Large tables (50+ GB). Lower concurrency prevents Storage Write API throttling and GCS read contention. |
| **EU Wave** | 13 tables: all `regional.*` | 4-5 concurrent | 4 workers, `n2-standard-8` equivalent | Separate Dataproc in `europe-west1`. Runs in parallel with US waves. |

### Wave Dependencies

```
distcp_phase (all 3 clusters in parallel)
    |
    +---> load_wave_1 -----> load_wave_2 -----> load_wave_3
    |                                                |
    +---> load_eu_tables (parallel with US waves)    |
                |                                    |
                +---> inline_validate_eu             +---> record_watermark
                                                          |
                                                          +---> notify_complete
```

Waves 1/2/3 are sequential (not parallel) to control total Dataproc resource consumption and Storage Write API load. The EU wave runs independently and in parallel with US waves since it uses a different project, region, and Dataproc cluster.

### DistCp Throughput

| Cluster | Estimated Data | DistCp Config | Target Duration |
|---|---|---|---|
| acme-lake (raw + staging) | ~500 GB - 2 TB | 20 map tasks, bandwidth limit 500 MB/s | 1-4 hours |
| acme-analytics (retail) | ~1-5 TB | 30 map tasks, bandwidth limit 500 MB/s | 2-8 hours |
| acme-edge (regional) | ~100-500 GB | 10 map tasks, bandwidth limit 200 MB/s | 1-2 hours |

All 3 DistCp jobs run in parallel. Total DistCp phase: bounded by the largest cluster (acme-analytics), estimated 2-8 hours depending on data volume.

### BigQuery Storage Write API Considerations

| Constraint | Limit | Mitigation |
|---|---|---|
| Concurrent streams per project | 10,000 | Well within limits — each Spark executor opens ~1 stream per partition write |
| Throughput per project | 3 GB/s default | Wave 3 concurrency of 3-4 keeps aggregate write rate within quota. Request quota increase if needed. |
| Append quota per table | Unlimited for committed mode | Using `writeMethod=direct` (committed mode) — no streaming buffer limitations |
| Row size limit | 10 MB per row | No tables approach this — largest row is `omniture_logs` at ~61 STRING columns |

### Spark Job Configuration

```python
# Common spark-bigquery-connector settings for all tables
spark.conf.set("spark.datasource.bigquery.writeMethod", "direct")
spark.conf.set("spark.datasource.bigquery.temporaryGcsBucket", staging_bucket)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# For large tables (Wave 3) — increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.datasource.bigquery.writeAtLeastOneRecord", "false")
```

### Estimated Total Pipeline Duration

| Phase | Estimated Duration |
|---|---|
| DistCp (all 3 clusters parallel) | 2-8 hours |
| Wave 1 — small tables (35 tables, 10-15 concurrent) | 30-60 minutes |
| Wave 2 — medium tables (30 tables, 5-8 concurrent) | 1-3 hours |
| Wave 3 — large tables (17 tables, 3-4 concurrent) | 3-8 hours |
| EU wave (13 tables, parallel with US) | 1-2 hours |
| Inline validation | 30-60 minutes |
| **Total estimated** | **8-20 hours** |

### Cost Controls

- **Dataproc Serverless**: Pay per vCPU-hour, no idle cluster costs. Jobs auto-terminate on completion.
- **GCS staging buckets**: Set lifecycle rule to auto-delete objects after 30 days (migration staging data is ephemeral).
- **Storage Write API**: No additional cost beyond standard BigQuery storage ingestion pricing.
- **Composer**: Uses existing Composer environment (provisioned for Oozie migration per locked decision). No incremental cost for the bulk load DAG.
