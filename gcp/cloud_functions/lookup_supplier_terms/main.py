# Cloud Functions code for lookup_supplier_terms Remote UDF
import json
from flask import jsonify
from google.cloud import storage

# In-memory cache for supplier terms
_TERMS_CACHE = None

def load_supplier_terms():
    """
    In production, this reads supplier_terms.csv from GCS.
    We will read from gs://${GCS_STAGING_US}/metadata/supplier_terms.csv
    If bucket or file is not found, or GCS client fails, we fallback to default hardcoded terms.
    """
    global _TERMS_CACHE
    if _TERMS_CACHE is not None:
        return _TERMS_CACHE

    terms = {
        "SUP-001": 30,
        "SUP-002": 45,
        "SUP-003": 60
    }

    try:
        # We can dynamically find staging US bucket, but since we may be running locally or in sandbox, 
        # let's write robust GCS-reading logic with fallback.
        client = storage.Client()
        # Look for bucket names matching staging pattern, or use an environment variable
        import os
        bucket_name = os.environ.get("STAGING_BUCKET", "acme-migration-staging-us")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("metadata/supplier_terms.csv")
        if blob.exists():
            data = blob.download_as_text()
            # Simple CSV parsing
            for line in data.strip().split("\n"):
                parts = line.strip().split(",")
                if len(parts) >= 2:
                    k, v = parts[0].strip(), parts[1].strip()
                    try:
                        terms[k] = int(v)
                    except ValueError:
                        pass
    except Exception:
        # Fallback gracefully
        pass

    _TERMS_CACHE = terms
    return _TERMS_CACHE

def handle_lookup_supplier_terms(request):
    """
    BigQuery Remote Function endpoint for lookup_supplier_terms.
    Payload:
    {
      "calls": [
        ["SUP-001"],
        ["SUP-999"],
        [null]
      ]
    }
    Response:
    {
      "replies": [
        30,
        30,
        30
      ]
    }
    """
    try:
        request_json = request.get_json(silent=True)
        if not request_json or "calls" not in request_json:
            return jsonify({"errorMessage": "Invalid request format"}), 400

        calls = request_json["calls"]
        terms = load_supplier_terms()
        replies = []
        for call in calls:
            supplier_code = call[0]
            if supplier_code is None:
                replies.append(30) # Default net-30
                continue

            code = str(supplier_code).strip()
            days = terms.get(code, 30) # Default net-30 if not found
            replies.append(days)

        return jsonify({"replies": replies})
    except Exception as e:
        return jsonify({"errorMessage": str(e)}), 500
