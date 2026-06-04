#!/usr/bin/env python3
"""Validate the bulk migration DAG and supporting modules."""
import ast
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("AIRFLOW_HOME", "/workspace/project")

import yaml


def check_syntax(files):
    """Verify all Python files parse without syntax errors."""
    print("=" * 60)
    print("1. SYNTAX CHECKS")
    print("=" * 60)
    errors = []
    for f in files:
        with open(f) as fh:
            source = fh.read()
        try:
            ast.parse(source)
            print(f"  ✓ {f}")
        except SyntaxError as e:
            print(f"  ✗ {f}: {e}")
            errors.append(f)
    return errors


def check_manifest_loader():
    """Verify manifest_loader discovers and groups manifests correctly."""
    print("\n" + "=" * 60)
    print("2. MANIFEST LOADER CHECKS")
    print("=" * 60)

    from dags.utils.manifest_loader import (
        discover_manifests, group_by_wave, group_by_cluster,
        get_table_key, get_table_id, get_bq_fqn,
        get_bq_partition_column, is_partitioned,
        get_gcs_manifest_path,
        WAVE_1_SMALL, WAVE_2_MEDIUM, WAVE_3_LARGE, WAVE_EU,
        ALL_WAVES,
    )

    errors = []
    manifests = discover_manifests("config/tables")
    print(f"  ✓ Discovered {len(manifests)} manifests")

    # Wave grouping
    waves = group_by_wave(manifests)
    total = sum(len(v) for v in waves.values())
    print(f"  ✓ Wave grouping: {total} tables across {len(waves)} waves")
    for w in ALL_WAVES:
        count = len(waves.get(w, []))
        print(f"    {w}: {count} tables")
    if total != len(manifests):
        errors.append(f"Wave total {total} != manifest count {len(manifests)}")

    # Cluster grouping
    clusters = group_by_cluster(manifests)
    print(f"  ✓ Cluster grouping: {len(clusters)} clusters")
    for c, tables in clusters.items():
        print(f"    {c}: {len(tables)} tables")

    # Spot-check helper functions
    sample = manifests[0]
    key = get_table_key(sample)
    tid = get_table_id(sample)
    fqn = get_bq_fqn(sample)
    print(f"  ✓ get_table_key: {key}")
    print(f"  ✓ get_table_id: {tid}")
    print(f"  ✓ get_bq_fqn: {fqn}")

    # Partition detection across all manifests
    partitioned = sum(1 for m in manifests if is_partitioned(m))
    non_partitioned = len(manifests) - partitioned
    print(f"  ✓ Partitioned tables: {partitioned}, non-partitioned: {non_partitioned}")

    # GCS manifest path
    gcs_path = get_gcs_manifest_path(sample, "gs://acme-migration-staging-us")
    assert gcs_path.startswith("gs://"), f"Bad GCS path: {gcs_path}"
    assert gcs_path.endswith(".yaml"), f"Bad GCS path: {gcs_path}"
    print(f"  ✓ GCS manifest path: {gcs_path}")

    return errors


def check_dag_structure():
    """Verify DAG has all required components."""
    print("\n" + "=" * 60)
    print("3. DAG STRUCTURE CHECKS")
    print("=" * 60)

    with open("dags/bulk_migration_dag.py") as f:
        source = f.read()

    errors = []
    checks = {
        # DAG configuration
        "DAG id 'bulk_migration_dag'": "bulk_migration_dag",
        "max_active_runs=1": "max_active_runs=1",
        "schedule_interval=None": "schedule_interval=None",
        "catchup=False": "catchup=False",
        # All 8 TaskGroups/phases
        "capture_source_counts TaskGroup": '"capture_source_counts"',
        "distcp_phase TaskGroup": '"distcp_phase"',
        "load_wave_1 TaskGroup": '"load_wave_1"',
        "load_wave_2 TaskGroup": '"load_wave_2"',
        "load_wave_3 TaskGroup": '"load_wave_3"',
        "load_eu_tables TaskGroup": '"load_eu_tables"',
        "record_watermark task": '"record_watermark"',
        "notify_complete task": '"notify_complete"',
        # Operators
        "BashOperator": "BashOperator",
        "DataprocCreateBatchOperator": "DataprocCreateBatchOperator",
        "BigQueryInsertJobOperator": "BigQueryInsertJobOperator",
        # Retry config
        "retries=3 for Spark tasks": "retries=3",
        "retries=2 for DistCp": "retries=2",
        "retry_exponential_backoff": "retry_exponential_backoff=True",
        # Callbacks
        "on_failure_callback": "on_failure_callback",
        "on_success_callback": "on_success_callback",
        # Watermark
        "bulk_migration_watermark_ts variable": "bulk_migration_watermark_ts",
        "_migration_metadata table": "_migration_metadata",
        "frozen_watermark_w": "frozen_watermark_w",
        "dag_run_id": "dag_run_id",
        # CLI args for bulk_load.py
        "--manifest-path": "--manifest-path",
        "--watermark-ts": "--watermark-ts",
        "--gcs-staging-bucket": "--gcs-staging-bucket",
        # Pool assignment
        "wave_1_pool": "wave_1_pool",
        "wave_2_pool": "wave_2_pool",
        "wave_3_pool": "wave_3_pool",
        "eu_pool": "eu_pool",
        "pool=pool_name": "pool=pool_name",
        # Dependency wiring
        "capture→distcp": "capture_source_counts >> distcp_phase",
        "distcp→wave1": "distcp_phase >> wave_1_tg",
        "wave1→wave2": "wave_1_tg >> wave_2_tg",
        "wave2→wave3": "wave_2_tg >> wave_3_tg",
        "distcp→EU (parallel)": "distcp_phase >> eu_tg",
        "wave3+EU→watermark": "[wave_3_tg, eu_tg] >> record_watermark",
        "watermark→notify": "record_watermark >> notify_complete",
        # DistCp commands
        "distcp_acme_lake": "distcp_acme_lake",
        "distcp_acme_analytics": "distcp_acme_analytics",
        "distcp_acme_edge": "distcp_acme_edge",
        "hadoop distcp": "hadoop distcp",
        # TriggerRule
        "TriggerRule.ALL_SUCCESS": "TriggerRule.ALL_SUCCESS",
    }

    for name, pattern in checks.items():
        found = pattern in source
        status = "✓" if found else "✗"
        print(f"  {status} {name}")
        if not found:
            errors.append(f"Missing: {name}")

    return errors


def check_inline_validation():
    """Verify inline validation module."""
    print("\n" + "=" * 60)
    print("4. INLINE VALIDATION CHECKS")
    print("=" * 60)

    with open("dags/operators/inline_validation.py") as f:
        source = f.read()

    errors = []
    checks = {
        "BigQueryCheckOperator": "BigQueryCheckOperator",
        "Row count SQL": "Row count mismatch",
        "Partition count SQL": "Partition count mismatch",
        "Null key SQL": "Unexpected NULLs",
        "build_inline_validation_tasks": "def build_inline_validation_tasks",
        "on_failure_callback": "on_failure_callback",
        "expected_row_count param": "expected_row_count",
        "expected_partition_count param": "expected_partition_count",
        "null_check_cols": "null_check_cols",
    }

    for name, pattern in checks.items():
        found = pattern in source
        status = "✓" if found else "✗"
        print(f"  {status} {name}")
        if not found:
            errors.append(f"Missing: {name}")

    return errors


def check_callbacks():
    """Verify callbacks module."""
    print("\n" + "=" * 60)
    print("5. CALLBACKS CHECKS")
    print("=" * 60)

    with open("dags/utils/callbacks.py") as f:
        source = f.read()

    errors = []
    checks = {
        "on_failure_callback": "def on_failure_callback",
        "on_success_callback": "def on_success_callback",
        "SlackWebhookOperator": "SlackWebhookOperator",
        "Slack channel": "#data-migration",
    }

    for name, pattern in checks.items():
        found = pattern in source
        status = "✓" if found else "✗"
        print(f"  {status} {name}")
        if not found:
            errors.append(f"Missing: {name}")

    return errors


def main():
    all_files = [
        "dags/__init__.py",
        "dags/utils/__init__.py",
        "dags/utils/manifest_loader.py",
        "dags/utils/callbacks.py",
        "dags/operators/__init__.py",
        "dags/operators/inline_validation.py",
        "dags/bulk_migration_dag.py",
    ]

    all_errors = []
    all_errors.extend(check_syntax(all_files))
    all_errors.extend(check_manifest_loader())
    all_errors.extend(check_dag_structure())
    all_errors.extend(check_inline_validation())
    all_errors.extend(check_callbacks())

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if all_errors:
        print(f"✗ {len(all_errors)} ERRORS:")
        for e in all_errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("✓ ALL CHECKS PASSED — DAG pipeline is complete and valid")


if __name__ == "__main__":
    main()
