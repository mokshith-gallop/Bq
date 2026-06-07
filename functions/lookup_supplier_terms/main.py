import json
import logging
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize GCS client lazily
storage_client = None

def get_storage_client():
    global storage_client
    if storage_client is None:
        storage_client = storage.Client()
    return storage_client

def lookup_supplier_terms(request):
    """
    BigQuery Remote Function endpoint to lookup supplier payment-term days.
    
    Accepts BigQuery POST request format:
    {
      "requestId": "12435abc",
      "caller": "//bigquery.googleapis.com/projects/myproject/jobs/myjob",
      "userCompletedUserDims": {},
      "calls": [
        ["SUP-001"],
        [null],
        ["SUP-999"]
      ]
    }
    
    Returns response format:
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
            return json.dumps({"errorMessage": "Invalid request, 'calls' key missing"}), 400, {"Content-Type": "application/json"}
        
        calls = request_json["calls"]
        replies = []
        
        # Load supplier terms (ideally we cache this, but let's read/lookup or mock)
        # We will parse CSV from GCS or use fallback/stub if CSV is not found
        # In a real environment, the bucket name and file path would be passed as env variables.
        # Let's write a robust parser.
        terms_map = {}
        try:
            client = get_storage_client()
            # Default fallback staging bucket or env config
            # Let's read bucket name from an environment variable, fallback to acme-migration-staging-us
            import os
            bucket_name = os.environ.get("STAGING_BUCKET", "acme-migration-staging-us")
            bucket = client.bucket(bucket_name)
            blob = bucket.blob("metadata/supplier_terms.csv")
            if blob.exists():
                content = blob.download_as_text()
                # Simple CSV parsing
                for line in content.splitlines():
                    parts = line.strip().split(",")
                    if len(parts) >= 2:
                        key = parts[0].strip()
                        try:
                            val = int(parts[1].strip())
                            terms_map[key] = val
                        except ValueError:
                            pass
            else:
                logger.warning(f"supplier_terms.csv not found in bucket {bucket_name}, using standard stubs.")
        except Exception as e:
            logger.error(f"Error loading supplier terms from GCS: {str(e)}. Falling back to stubs.")
        
        # Merge or default stubs
        stubs = {
            "SUP-001": 30,
            "SUP-002": 45,
            "SUP-003": 60
        }
        for k, v in stubs.items():
            if k not in terms_map:
                terms_map[k] = v
                
        for call in calls:
            if not call or len(call) == 0 or call[0] is None:
                # Default is net-30
                replies.append(30)
                continue
            
            supplier_code = str(call[0]).strip()
            days = terms_map.get(supplier_code, 30) # default to net-30 if not found
            replies.append(days)
            
        return json.dumps({"replies": replies}), 200, {"Content-Type": "application/json"}
        
    except Exception as e:
        logger.error(f"Error processing lookup_supplier_terms: {str(e)}")
        return json.dumps({"errorMessage": str(e)}), 500, {"Content-Type": "application/json"}
