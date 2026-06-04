"""
bulk_migration_dag.py — Cloud Composer DAG for bulk historical data migration.

Orchestrates the complete migration of 82+ Hive tables across 3 on-prem
Cloudera clusters into BigQuery:

  Phase 1 — capture_source_counts: SSH to each cluster, run COUNT(*) on all
            Hive tables with watermark filter, write to GCS manifest.
  Phase 2 — distcp_phase: 3 parallel DistCp jobs from HDFS to GCS.
  Phase 3 — load_wave_1 → load_wave_2 → load_wave_3: Sequential US waves
            with Dataproc Serverless PySpark jobs. EU wave runs in parallel.
  Phase 4 — record_watermark: Write frozen watermark W to BQ metadata table.
  Phase 5 — notify_complete: Slack notification.

Dependencies:
  capture_source_counts → distcp_phase → load_wave_1 → load_wave_2 → load_wave_3
                                 ↘                                         ↓
                              load_eu_tables ───────────→ record_watermark
                                                               ↓
                                                         notify_complete

Configuration:
  - Watermark W: Airflow Variable `bulk_migration_watermark_ts`
  - All table configs: YAML manifests in config/tables/{raw,staging,retail,regional}/
  - Pools: wave_1_pool (15 slots), wave_2_pool (8 slots),
           wave_3_pool (4 slots), eu_pool (5 slots)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dags.operators.inline_validation import build_inline_validation_tasks
from dags.utils.callbacks import on_failure_callback, on_success_callback
from dags.utils.manifest_loader import (
    WAVE_1_SMALL,
    WAVE_2_MEDIUM,
    WAVE_3_LARGE,
    WAVE_EU,
    discover_manifests,
    get_gcs_manifest_path,
    get_table_id,
    group_by_wave,
)

# ============================================================================
# Constants
# ============================================================================

# GCP Project & Region
PROJECT_US = "acme-analytics"
PROJECT_EU = "acme-analytics-eu"
REGION_US = "us-central1"
REGION_EU = "europe-west1"

# GCS Buckets
GCS_STAGING_US = "acme-migration-staging-us"
GCS_STAGING_EU = "acme-migration-staging-eu"

# Dataproc Serverless config
SPARK_JOB_FILE = f"gs://{GCS_STAGING_US}/spark/bulk_load.py"
SPARK_BQ_CONNECTOR = (
    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
)
SPARK_AVRO_PACKAGE = "org.apache.spark:spark-avro_2.12:3.5.1"

# Composer / Airflow connection
GCP_CONN_ID = "google_cloud_default"

# Source clusters for DistCp / SSH
CLUSTER_ACME_LAKE = "acme-lake"
CLUSTER_ACME_ANALYTICS = "acme-analytics"
CLUSTER_ACME_EDGE = "acme-edge"

# Cluster SSH gateway hosts (Composer → on-prem via Cloud Interconnect)
CLUSTER_SSH_HOSTS = {
    CLUSTER_ACME_LAKE: "edge-node.acme-lake.internal",
    CLUSTER_ACME_ANALYTICS: "edge-node.acme-analytics.internal",
    CLUSTER_ACME_EDGE: "edge-node.acme-edge.internal",
}

# Config root — in Composer, DAGs and config/ are synced to GCS
CONFIG_ROOT = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/home/airflow/gcs"),
    "dags",
    "config",
    "tables",
)

# ============================================================================
# Dataproc Serverless batch configurations per wave
# ============================================================================

_DATAPROC_BASE_PROPERTIES = {
    "spark.datasource.bigquery.writeMethod": "direct",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}


def _wave_1_dataproc_config() -> Dict[str, Any]:
    """Small tables: 2 workers, n2-standard-4."""
    return {
        "runtime_config": {
            "version": "2.1",
            "properties": {
                **_DATAPROC_BASE_PROPERTIES,
                "spark.executor.instances": "2",
                "spark.executor.cores": "4",
                "spark.executor.memory": "8g",
                "spark.driver.memory": "4g",
            },
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": f"projects/{PROJECT_US}/regions/{REGION_US}/subnetworks/default",
            }
        },
    }


def _wave_2_dataproc_config() -> Dict[str, Any]:
    """Medium tables: 4 workers, n2-standard-8."""
    return {
        "runtime_config": {
            "version": "2.1",
            "properties": {
                **_DATAPROC_BASE_PROPERTIES,
                "spark.executor.instances": "4",
                "spark.executor.cores": "8",
                "spark.executor.memory": "16g",
                "spark.driver.memory": "8g",
            },
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": f"projects/{PROJECT_US}/regions/{REGION_US}/subnetworks/default",
            }
        },
    }


def _wave_3_dataproc_config() -> Dict[str, Any]:
    """Large tables: 8-16 workers, n2-standard-16, autoscaling."""
    return {
        "runtime_config": {
            "version": "2.1",
            "properties": {
                **_DATAPROC_BASE_PROPERTIES,
                "spark.executor.instances": "8",
                "spark.executor.cores": "16",
                "spark.executor.memory": "32g",
                "spark.driver.memory": "16g",
                "spark.sql.shuffle.partitions": "200",
                "spark.datasource.bigquery.writeAtLeastOneRecord": "false",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "16",
            },
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": f"projects/{PROJECT_US}/regions/{REGION_US}/subnetworks/default",
            }
        },
    }


def _eu_dataproc_config() -> Dict[str, Any]:
    """EU tables: 4 workers, n2-standard-8 in europe-west1."""
    return {
        "runtime_config": {
            "version": "2.1",
            "properties": {
                **_DATAPROC_BASE_PROPERTIES,
                "spark.executor.instances": "4",
                "spark.executor.cores": "8",
                "spark.executor.memory": "16g",
                "spark.driver.memory": "8g",
            },
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": f"projects/{PROJECT_EU}/regions/{REGION_EU}/subnetworks/default",
            }
        },
    }


_WAVE_CONFIGS = {
    WAVE_1_SMALL: _wave_1_dataproc_config,
    WAVE_2_MEDIUM: _wave_2_dataproc_config,
    WAVE_3_LARGE: _wave_3_dataproc_config,
    WAVE_EU: _eu_dataproc_config,
}

# Pool names (must be created in Airflow Admin → Pools)
_WAVE_POOLS = {
    WAVE_1_SMALL: "wave_1_pool",
    WAVE_2_MEDIUM: "wave_2_pool",
    WAVE_3_LARGE: "wave_3_pool",
    WAVE_EU: "eu_pool",
}

# ============================================================================
# Helper functions
# ============================================================================


def _build_dataproc_pyspark_job(
    manifest: Dict[str, Any],
    wave: str,
    watermark_ts: str,
) -> Dict[str, Any]:
    """
    Build a Dataproc Serverless PySpark batch job spec for a single table.

    The job runs bulk_load.py with the table's manifest path and watermark.
    """
    table_id = get_table_id(manifest)
    gcs_config_root = (
        f"gs://{GCS_STAGING_EU}" if wave == WAVE_EU else f"gs://{GCS_STAGING_US}"
    )
    manifest_gcs_path = get_gcs_manifest_path(manifest, gcs_config_root)
    staging_bucket = GCS_STAGING_EU if wave == WAVE_EU else GCS_STAGING_US

    dataproc_config = _WAVE_CONFIGS[wave]()

    # Determine required packages
    packages = [SPARK_BQ_CONNECTOR]
    source_format = manifest.get("source", {}).get("format", "")
    if source_format == "avro":
        packages.append(SPARK_AVRO_PACKAGE)

    job = {
        "pyspark_batch": {
            "main_python_file_uri": SPARK_JOB_FILE,
            "args": [
                "--manifest-path", manifest_gcs_path,
                "--watermark-ts", watermark_ts,
                "--gcs-staging-bucket", staging_bucket,
            ],
            "jar_file_uris": [],
        },
        **dataproc_config,
    }

    # Add packages via spark properties
    job["runtime_config"]["properties"]["spark.jars.packages"] = ",".join(packages)

    return job


def _build_beeline_count_command(
    database: str,
    table: str,
    watermark_col: str | None,
    watermark_ts: str,
    cluster_host: str,
) -> str:
    """
    Build a beeline command to count rows in a Hive table.

    Returns a shell command that SSHs to the cluster edge node and runs
    the count query via beeline.
    """
    if watermark_col:
        query = (
            f"SELECT '{database}.{table}' AS tbl, COUNT(*) AS cnt "
            f"FROM {database}.{table} "
            f"WHERE {watermark_col} <= '{watermark_ts}'"
        )
    else:
        query = (
            f"SELECT '{database}.{table}' AS tbl, COUNT(*) AS cnt "
            f"FROM {database}.{table}"
        )

    return (
        f"ssh -o StrictHostKeyChecking=no {cluster_host} "
        f"\"beeline -u 'jdbc:hive2://localhost:10000/{database}' "
        f"-e \\\"{query}\\\" --outputformat=csv2 2>/dev/null | tail -1\""
    )


def _build_distcp_command(
    source_cluster: str,
    hdfs_paths: List[str],
    gcs_dest: str,
    map_tasks: int,
    bandwidth_mb: int,
) -> str:
    """
    Build a DistCp shell command to copy HDFS data to GCS.

    Runs on the source cluster edge node via SSH.
    """
    cluster_host = CLUSTER_SSH_HOSTS[source_cluster]
    hdfs_src_args = " ".join(hdfs_paths)

    return (
        f"ssh -o StrictHostKeyChecking=no {cluster_host} "
        f"\"hadoop distcp "
        f"-Dmapreduce.job.maps={map_tasks} "
        f"-bandwidth {bandwidth_mb} "
        f"-overwrite "
        f"-strategy dynamic "
        f"-log /tmp/distcp_log_{source_cluster} "
        f"{hdfs_src_args} "
        f"{gcs_dest}\""
    )


# ============================================================================
# DAG Definition
# ============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # Per-task overrides set below
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=12),
    "on_failure_callback": on_failure_callback,
}


with DAG(
    dag_id="bulk_migration_dag",
    default_args=default_args,
    description=(
        "Bulk historical data migration: 82+ Hive tables across 3 Cloudera "
        "clusters → BigQuery via DistCp + Dataproc Serverless PySpark."
    ),
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migration", "bulk-load", "hive-to-bigquery"],
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # Read configuration from Airflow Variables
    # -----------------------------------------------------------------------
    watermark_ts = Variable.get(
        "bulk_migration_watermark_ts",
        default_var="2024-06-01T00:00:00Z",
    )

    source_counts_gcs = (
        f"gs://{GCS_STAGING_US}/manifests/source_counts.json"
    )

    # -----------------------------------------------------------------------
    # Discover and group table manifests
    # -----------------------------------------------------------------------
    all_manifests = discover_manifests(CONFIG_ROOT)
    wave_groups = group_by_wave(all_manifests)

    # =====================================================================
    # Phase 1: Capture Source Counts
    # =====================================================================
    with TaskGroup("capture_source_counts", tooltip="Capture row counts from source Hive tables") as capture_source_counts:

        # Build beeline count commands per cluster
        cluster_tables: Dict[str, List[Dict[str, Any]]] = {
            CLUSTER_ACME_LAKE: [],
            CLUSTER_ACME_ANALYTICS: [],
            CLUSTER_ACME_EDGE: [],
        }
        for m in all_manifests:
            cluster = m.get("source", {}).get("cluster", CLUSTER_ACME_LAKE)
            if cluster in cluster_tables:
                cluster_tables[cluster].append(m)

        # Generate a script that runs all beeline counts and writes JSON
        def _build_count_script(cluster: str, tables: List[Dict[str, Any]]) -> str:
            """Generate a shell script that runs COUNT(*) per table via beeline."""
            cluster_host = CLUSTER_SSH_HOSTS[cluster]
            lines = [
                "#!/bin/bash",
                "set -euo pipefail",
                f'echo "Capturing counts from {cluster} ({len(tables)} tables)..."',
                f"OUTFILE=/tmp/counts_{cluster}.json",
                'echo "{" > $OUTFILE',
            ]
            for i, m in enumerate(tables):
                db = m.get("source", {}).get("database", "")
                tbl = m.get("source", {}).get("table", "")
                wm_col = m.get("validation", {}).get("watermark_col")
                if wm_col:
                    query = (
                        f"SELECT COUNT(*) FROM {db}.{tbl} "
                        f"WHERE {wm_col} <= '{watermark_ts}'"
                    )
                else:
                    query = f"SELECT COUNT(*) FROM {db}.{tbl}"

                comma = "," if i < len(tables) - 1 else ""
                lines.append(
                    f'CNT=$(ssh -o StrictHostKeyChecking=no {cluster_host} '
                    f'"beeline -u \'jdbc:hive2://localhost:10000/{db}\' '
                    f"-e \\\"{query}\\\" --outputformat=csv2 2>/dev/null "
                    f'| tail -1" || echo "0")'
                )
                lines.append(
                    f'echo "  \\"{db}.{tbl}\\": $CNT{comma}" >> $OUTFILE'
                )
            lines.append('echo "}" >> $OUTFILE')
            lines.append(f"cat $OUTFILE")
            return "\n".join(lines)

        count_tasks = []
        for cluster, tables in cluster_tables.items():
            if not tables:
                continue
            count_task = BashOperator(
                task_id=f"count_{cluster.replace('-', '_')}",
                bash_command=_build_count_script(cluster, tables),
                retries=2,
                retry_delay=timedelta(minutes=10),
            )
            count_tasks.append(count_task)

        # Merge per-cluster count files into a single manifest and upload to GCS
        merge_counts = BashOperator(
            task_id="merge_and_upload_counts",
            bash_command=(
                "python3 -c \""
                "import json, glob; "
                "merged = {}; "
                "[merged.update(json.load(open(f))) for f in glob.glob('/tmp/counts_*.json')]; "
                "json.dump(merged, open('/tmp/source_counts.json', 'w')); "
                f"\" && gsutil cp /tmp/source_counts.json {source_counts_gcs}"
            ),
            retries=1,
            retry_delay=timedelta(minutes=5),
        )

        for ct in count_tasks:
            ct >> merge_counts

    # =====================================================================
    # Phase 2: DistCp — HDFS to GCS (3 clusters in parallel)
    # =====================================================================
    with TaskGroup("distcp_phase", tooltip="DistCp from HDFS to GCS") as distcp_phase:

        distcp_acme_lake = BashOperator(
            task_id="distcp_acme_lake",
            bash_command=_build_distcp_command(
                source_cluster=CLUSTER_ACME_LAKE,
                hdfs_paths=[
                    "/user/etl/raw/",
                    "/user/hive/warehouse/staging.db/",
                ],
                gcs_dest=f"gs://{GCS_STAGING_US}/",
                map_tasks=20,
                bandwidth_mb=500,
            ),
            retries=2,
            retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(hours=8),
        )

        distcp_acme_analytics = BashOperator(
            task_id="distcp_acme_analytics",
            bash_command=_build_distcp_command(
                source_cluster=CLUSTER_ACME_ANALYTICS,
                hdfs_paths=["/user/hive/warehouse/retail.db/"],
                gcs_dest=f"gs://{GCS_STAGING_US}/retail/",
                map_tasks=30,
                bandwidth_mb=500,
            ),
            retries=2,
            retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(hours=12),
        )

        distcp_acme_edge = BashOperator(
            task_id="distcp_acme_edge",
            bash_command=_build_distcp_command(
                source_cluster=CLUSTER_ACME_EDGE,
                hdfs_paths=["/user/hive/warehouse/regional.db/"],
                gcs_dest=f"gs://{GCS_STAGING_EU}/regional/",
                map_tasks=10,
                bandwidth_mb=200,
            ),
            retries=2,
            retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(hours=4),
        )

    # =====================================================================
    # Phase 3: Wave-based loading via Dataproc Serverless
    # =====================================================================

    def _create_wave_task_group(
        wave_name: str,
        manifests: List[Dict[str, Any]],
        group_id: str,
        tooltip: str,
    ) -> TaskGroup:
        """
        Create a TaskGroup for a loading wave.

        Each table gets:
          1. A DataprocCreateBatchOperator to run bulk_load.py
          2. Inline validation tasks (row count, null key checks)

        All load tasks within a wave run concurrently, controlled by the
        Airflow pool slot limit.
        """
        with TaskGroup(group_id, tooltip=tooltip) as tg:
            pool_name = _WAVE_POOLS[wave_name]
            project = PROJECT_EU if wave_name == WAVE_EU else PROJECT_US
            region = REGION_EU if wave_name == WAVE_EU else REGION_US

            for manifest in manifests:
                table_id = get_table_id(manifest)
                src = manifest.get("source", {})
                db = src.get("database", "unknown")
                tbl = src.get("table", "unknown")

                # ----------------------------------------------------------
                # Load task: Dataproc Serverless PySpark batch
                # ----------------------------------------------------------
                job_spec = _build_dataproc_pyspark_job(
                    manifest=manifest,
                    wave=wave_name,
                    watermark_ts=watermark_ts,
                )

                batch_id = f"bulk-load-{db}-{tbl}-{{{{ ds_nodash }}}}"
                load_task = DataprocCreateBatchOperator(
                    task_id=f"load__{db}__{tbl}",
                    batch=job_spec,
                    batch_id=batch_id,
                    project_id=project,
                    region=region,
                    gcp_conn_id=GCP_CONN_ID,
                    pool=pool_name,
                    retries=3,
                    retry_delay=timedelta(minutes=5),
                    retry_exponential_backoff=True,
                    max_retry_delay=timedelta(minutes=30),
                    execution_timeout=timedelta(hours=6),
                    on_failure_callback=on_failure_callback,
                )

                # ----------------------------------------------------------
                # Inline validation tasks
                # ----------------------------------------------------------
                # We build validation tasks; expected_row_count will be
                # populated at runtime from the source_counts.json manifest.
                # For DAG construction, we pass None and rely on the
                # validation operator to read from XCom or the manifest.
                #
                # In practice, the row count check uses the value captured
                # in Phase 1. For DAG definition time, we build the tasks
                # with a placeholder — the actual SQL uses a subquery
                # against the _migration_source_counts table or a
                # runtime-resolved template.
                validation_tasks = build_inline_validation_tasks(
                    manifest=manifest,
                    expected_row_count=None,  # Resolved at runtime via source_counts
                    expected_partition_count=None,
                    gcp_conn_id=GCP_CONN_ID,
                )

                # Wire: load → all validation tasks (parallel validation)
                for vt in validation_tasks:
                    load_task >> vt

        return tg

    # Create US wave TaskGroups
    wave_1_tg = _create_wave_task_group(
        wave_name=WAVE_1_SMALL,
        manifests=wave_groups[WAVE_1_SMALL],
        group_id="load_wave_1",
        tooltip=f"Wave 1 — Small tables ({len(wave_groups[WAVE_1_SMALL])} tables, 10-15 concurrent)",
    )

    wave_2_tg = _create_wave_task_group(
        wave_name=WAVE_2_MEDIUM,
        manifests=wave_groups[WAVE_2_MEDIUM],
        group_id="load_wave_2",
        tooltip=f"Wave 2 — Medium tables ({len(wave_groups[WAVE_2_MEDIUM])} tables, 5-8 concurrent)",
    )

    wave_3_tg = _create_wave_task_group(
        wave_name=WAVE_3_LARGE,
        manifests=wave_groups[WAVE_3_LARGE],
        group_id="load_wave_3",
        tooltip=f"Wave 3 — Large tables ({len(wave_groups[WAVE_3_LARGE])} tables, 3-4 concurrent)",
    )

    # Create EU wave TaskGroup (runs in parallel with US waves)
    eu_tg = _create_wave_task_group(
        wave_name=WAVE_EU,
        manifests=wave_groups[WAVE_EU],
        group_id="load_eu_tables",
        tooltip=f"EU Wave — Regional tables ({len(wave_groups[WAVE_EU])} tables, 4-5 concurrent)",
    )

    # =====================================================================
    # Phase 4: Record Watermark
    # =====================================================================
    record_watermark = BigQueryInsertJobOperator(
        task_id="record_watermark",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_US}.raw._migration_metadata` AS
                    SELECT
                        CURRENT_TIMESTAMP() AS migration_completed_at,
                        TIMESTAMP '{watermark_ts}' AS frozen_watermark_w,
                        {len(all_manifests)} AS total_tables_loaded,
                        'bulk_migration_dag' AS dag_id,
                        '{{{{ run_id }}}}' AS dag_run_id
                """,
                "useLegacySql": False,
            }
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    # =====================================================================
    # Phase 5: Notify Complete
    # =====================================================================
    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=on_success_callback,
        op_kwargs={},
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # =====================================================================
    # Wire DAG dependencies
    # =====================================================================

    # Phase 1 → Phase 2
    capture_source_counts >> distcp_phase

    # Phase 2 → Phase 3 (US waves are sequential; EU wave is parallel)
    distcp_phase >> wave_1_tg >> wave_2_tg >> wave_3_tg

    # EU wave branches from distcp and runs in parallel with US waves
    distcp_phase >> eu_tg

    # Phase 3 → Phase 4: Both US wave_3 and EU wave must complete
    [wave_3_tg, eu_tg] >> record_watermark

    # Phase 4 → Phase 5
    record_watermark >> notify_complete
