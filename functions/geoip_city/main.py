import json
import logging
import ipaddress

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def geoip_city(request):
    """
    BigQuery Remote Function endpoint to map IP addresses to coarse regions/cities.
    
    Accepts BigQuery POST request format:
    {
      "requestId": "12435abc",
      "caller": "//bigquery.googleapis.com/projects/myproject/jobs/myjob",
      "userCompletedUserDims": {},
      "calls": [
        ["10.0.0.1"],
        ["192.168.1.1"],
        ["8.8.8.8"],
        [null]
      ]
    }
    
    Returns response format:
    {
      "replies": [
        "Internal",
        "Internal",
        "Americas",
        null
      ]
    }
    """
    try:
        request_json = request.get_json(silent=True)
        if not request_json or "calls" not in request_json:
            return json.dumps({"errorMessage": "Invalid request, 'calls' key missing"}), 400, {"Content-Type": "application/json"}
        
        calls = request_json["calls"]
        replies = []
        
        for call in calls:
            if not call or len(call) == 0 or call[0] is None:
                replies.append(None)
                continue
            
            ip_str = str(call[0]).strip()
            
            # Simple ip parsing logic mimicking original UDF
            # original Java UDF logic:
            # int dot = ip.indexOf('.');
            # if (dot <= 0) return new Text("Unknown");
            # int firstOctet = Integer.parseInt(ip.substring(0, dot));
            # if (firstOctet == 10 || firstOctet == 192 || firstOctet == 172) return "Internal";
            # if (firstOctet < 64)  return "Americas";
            # if (firstOctet < 128) return "EMEA";
            # if (firstOctet < 192) return "APAC";
            # return "Other";
            
            try:
                # We can also check if it's a valid IP address. Let's make it robust.
                # If it's a private network IP, return "Internal"
                # Let's match the Java UDF precisely so parity testing passes!
                dot_idx = ip_str.find('.')
                if dot_idx <= 0:
                    replies.append("Unknown")
                    continue
                
                first_octet_str = ip_str[:dot_idx]
                first_octet = int(first_octet_str)
                
                if first_octet in (10, 192, 172):
                    replies.append("Internal")
                elif first_octet < 64:
                    replies.append("Americas")
                elif first_octet < 128:
                    replies.append("EMEA")
                elif first_octet < 192:
                    replies.append("APAC")
                else:
                    replies.append("Other")
            except Exception:
                replies.append("Unknown")
                
        return json.dumps({"replies": replies}), 200, {"Content-Type": "application/json"}
        
    except Exception as e:
        logger.error(f"Error processing geoip_city: {str(e)}")
        return json.dumps({"errorMessage": str(e)}), 500, {"Content-Type": "application/json"}
