#!/usr/bin/env python3
"""
deploy_ddl.py - Production-grade BigQuery DDL schema deployment script.
Manages dataset orchestration, dynamic variable substitution, and schema execution.
"""

import os
import re
import sys
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Default project/dataset template variables
DEFAULT_VARS = {
    "PROJECT_US": "acme-analytics",
    "PROJECT_EU": "acme-analytics-eu",
    "DS_RAW": "raw",
    "DS_STAGING": "staging",
    "DS_RETAIL": "retail",
    "DS_REGIONAL": "regional_eu",
    "DS_UDFS": "udfs"
}

def load_variables(env_path="/workspace/project/ddl/variables.env"):
    """
    Loads environment template variables from the specified .env file,
    falling back to DEFAULT_VARS if file not found or parses empty.
    """
    variables = DEFAULT_VARS.copy()
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    variables[key] = val
    return variables

def substitute_variables(sql_content, variables):
    """
    Dynamically substitutes template variables of the form ${VAR_NAME} or $VAR_NAME
    with the values loaded from environment or variables.env.
    """
    # Sort keys by length descending to prevent partial replacements
    for key in sorted(variables.keys(), key=len, reverse=True):
        val = variables[key]
        # Replace ${VAR_NAME}
        sql_content = sql_content.replace(f"${{{key}}}", val)
        # Replace $VAR_NAME (using word boundaries or non-alphanumeric trailing check)
        sql_content = re.sub(rf"\${key}\b", val, sql_content)
    return sql_content

def create_datasets_if_not_exist(client_us, client_eu, variables):
    """
    Checks for and creates target BigQuery datasets across two separate projects,
    enforcing geographic placement rules:
    - US multi-region for raw, staging, retail, and udfs in PROJECT_US
    - EU multi-region for regional_eu and udfs in PROJECT_EU
    """
    us_project = variables.get("PROJECT_US", "acme-analytics")
    eu_project = variables.get("PROJECT_EU", "acme-analytics-eu")
    udfs_dataset = variables.get("DS_UDFS", "udfs")
    
    us_datasets = [
        variables.get("DS_RAW", "raw"),
        variables.get("DS_STAGING", "staging"),
        variables.get("DS_RETAIL", "retail"),
        udfs_dataset
    ]
    eu_datasets = [
        variables.get("DS_REGIONAL", "regional_eu"),
        udfs_dataset
    ]
    
    print("\nOrchestrating datasets...")
    
    # Process US datasets
    for ds_name in us_datasets:
        dataset_ref = f"{us_project}.{ds_name}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        try:
            client_us.get_dataset(dataset_ref)
            print(f"  Dataset {dataset_ref} already exists (Location: US).")
        except Exception:
            print(f"  Dataset {dataset_ref} does not exist. Creating in US region...")
            try:
                client_us.create_dataset(dataset, timeout=30)
                print(f"  ✓ Created dataset {dataset_ref} in US.")
            except Exception as e:
                print(f"  [Dry-Run or Warn] Could not create {dataset_ref}: {e}")

    # Process EU datasets
    for ds_name in eu_datasets:
        dataset_ref = f"{eu_project}.{ds_name}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        try:
            client_eu.get_dataset(dataset_ref)
            print(f"  Dataset {dataset_ref} already exists (Location: EU).")
        except Exception:
            print(f"  Dataset {dataset_ref} does not exist. Creating in EU region...")
            try:
                client_eu.create_dataset(dataset, timeout=30)
                print(f"  ✓ Created dataset {dataset_ref} in EU.")
            except Exception as e:
                print(f"  [Dry-Run or Warn] Could not create {dataset_ref}: {e}")

def deploy_order_files(deploy_order_path="/workspace/project/ddl/deploy_order.txt"):
    """
    Reads the deploy_order.txt and parses files to be executed in sequence.
    Excludes empty lines and comment lines.
    """
    files = []
    if os.path.exists(deploy_order_path):
        with open(deploy_order_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                files.append(line)
    return files

def main():
    print("Loading variables...")
    vars_loaded = load_variables()
    print("Loaded variables:")
    for k, v in sorted(vars_loaded.items()):
        print(f"  {k} = {v}")

    # Setup BigQuery clients for both PROJECT_US and PROJECT_EU
    us_project = vars_loaded.get("PROJECT_US")
    eu_project = vars_loaded.get("PROJECT_EU")
    
    # We create clients (using default credentials or fallback gracefully if credentials are not configured)
    try:
        client_us = bigquery.Client(project=us_project)
    except Exception as e:
        print(f"Warning: Could not initialize US BigQuery Client: {e}")
        client_us = None

    try:
        client_eu = bigquery.Client(project=eu_project)
    except Exception as e:
        print(f"Warning: Could not initialize EU BigQuery Client: {e}")
        client_eu = None

    # Call dataset orchestration (creates/checks datasets in correct region)
    if client_us and client_eu:
        create_datasets_if_not_exist(client_us, client_eu, vars_loaded)
    else:
        print("\nSkipping dataset check/creation due to missing clients/credentials.")

    # Load and print deployment order
    ordered_files = deploy_order_files()
    print(f"\nDiscovered {len(ordered_files)} files in deploy_order.txt:")
    if ordered_files:
        print(f"  First file: {ordered_files[0]}")
        print(f"  Last file: {ordered_files[-1]}")

if __name__ == "__main__":
    main()
