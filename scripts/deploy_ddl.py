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
    # Sort keys by length descending to prevent partial replacements (e.g. PROJECT_US replacing parts)
    for key in sorted(variables.keys(), key=len, reverse=True):
        val = variables[key]
        # Replace ${VAR_NAME}
        sql_content = sql_content.replace(f"${{{key}}}", val)
        # Replace $VAR_NAME (using word boundaries or non-alphanumeric trailing check)
        sql_content = re.sub(rf"\${key}\b", val, sql_content)
    return sql_content

def main():
    print("Loading variables...")
    vars_loaded = load_variables()
    print("Loaded variables:")
    for k, v in sorted(vars_loaded.items()):
        print(f"  {k} = {v}")

if __name__ == "__main__":
    main()
