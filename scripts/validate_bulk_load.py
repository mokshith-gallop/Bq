#!/usr/bin/env python3
"""Validate bulk_load.py against all YAML manifests."""
import ast
import os
import re
import sys

import yaml


def main():
    errors = []

    # 1. Verify Python syntax
    with open("spark/bulk_load.py") as f:
        source = f.read()
    try:
        ast.parse(source)
        print("✓ Python syntax valid")
    except SyntaxError as e:
        print(f"✗ Syntax error: {e}")
        sys.exit(1)

    # 2. Extract registered formats from code
    # Supports both @FormatReaderRegistry.register("x") and @_register_reader("x")
    registered = re.findall(
        r'@(?:FormatReaderRegistry\.register|_register_reader)\(["\'](\w+)["\']\)',
        source,
    )
    print(f"\nRegistered handlers ({len(registered)}): {sorted(registered)}")

    # 3. Verify all manifest formats have handlers
    manifest_formats = set()
    manifest_count = 0
    for root, dirs, files in os.walk("config/tables"):
        for f in sorted(files):
            if not f.endswith(".yaml"):
                continue
            path = os.path.join(root, f)
            with open(path) as fh:
                m = yaml.safe_load(fh)
            manifest_formats.add(m["source"]["format"])
            manifest_count += 1

    print(f"Manifest formats ({len(manifest_formats)}): {sorted(manifest_formats)}")
    print(f"Total manifests: {manifest_count}")

    missing = manifest_formats - set(registered)
    if missing:
        errors.append(f"MISSING handlers for: {missing}")
    else:
        print(f"✓ All {len(manifest_formats)} manifest formats have handlers")

    # 4. Check field name alignment
    field_checks = [
        ("kudu_epoch_conversion", 'kudu_epoch_conversion'),
        ("acid_compaction", 'acid_compaction'),
        ("null_value", 'null_value'),
        ("regex_pattern", 'regex_pattern'),
        ("regex_columns", 'regex_columns'),
        ("generated_column", 'generated_column'),
        ("map_to_json", 'map_to_json'),
        ("source_col", 'source_col'),
        ("source_cols", 'source_cols'),
        ("target_col", 'target_col'),
        ("parse_fn", 'parse_fn'),
        ("from_unit", 'from_unit'),
        ("watermark_col", 'watermark_col'),
    ]
    print("\nField name checks:")
    for name, pattern in field_checks:
        if pattern in source:
            print(f"  ✓ {name}")
        else:
            errors.append(f"Field '{name}' not found in code")
            print(f"  ✗ {name} — NOT FOUND")

    # 5. Verify key functions exist
    tree = ast.parse(source)
    func_names = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    required_funcs = [
        "load_manifest",
        "build_spark_session",
        "apply_transforms",
        "_apply_watermark_filter",
        "_apply_partition_key_conversion",
        "_apply_map_to_json",
        "_apply_kudu_epoch_conversion",
        "_drop_generated_columns",
        "write_to_bigquery",
        "parse_args",
        "main",
    ]
    print("\nRequired functions:")
    for fn in required_funcs:
        if fn in func_names:
            print(f"  ✓ {fn}")
        else:
            errors.append(f"Missing function: {fn}")
            print(f"  ✗ {fn} — MISSING")

    # 6. Verify CLI args
    for arg in ["--manifest-path", "--watermark-ts", "--gcs-staging-bucket", "--dry-run"]:
        if arg in source:
            print(f"  ✓ CLI arg: {arg}")
        else:
            errors.append(f"Missing CLI arg: {arg}")

    # Summary
    if errors:
        print(f"\n✗ {len(errors)} ERRORS:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("\n✓ ALL VALIDATION CHECKS PASSED")


if __name__ == "__main__":
    main()
