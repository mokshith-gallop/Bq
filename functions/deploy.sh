#!/bin/bash
# Deployment script for Remote UDF Cloud Functions
# For local run or automated deployment.
# Usage: ./deploy.sh [PROJECT_ID] [REGION]

set -euo pipefail

PROJECT_ID=${1:-"acme-analytics"}
REGION=${2:-"us-central1"}

echo "Deploying Cloud Functions in project ${PROJECT_ID} and region ${REGION}..."

# Deploy lookup_supplier_terms
gcloud functions deploy lookup_supplier_terms \
    --runtime=python311 \
    --trigger-http \
    --allow-unauthenticated \
    --entry-point=lookup_supplier_terms \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --source=functions/lookup_supplier_terms

# Deploy geoip_city
gcloud functions deploy geoip_city \
    --runtime=python311 \
    --trigger-http \
    --allow-unauthenticated \
    --entry-point=geoip_city \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --source=functions/geoip_city

echo "Cloud Functions deployed successfully."
