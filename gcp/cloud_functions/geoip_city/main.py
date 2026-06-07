# Cloud Functions code for geoip_city Remote UDF
import json
from flask import jsonify

def handle_geoip_city(request):
    """
    BigQuery Remote Function endpoint for geoip_city.
    BigQuery sends a JSON payload:
    {
      "requestId": "...",
      "caller": "...",
      "userDefinedContext": {},
      "calls": [
        ["10.0.0.1"],
        ["192.168.1.1"],
        [null]
      ]
    }
    Expected response payload:
    {
      "replies": [
        "Internal",
        "Internal",
        null
      ]
    }
    """
    try:
        request_json = request.get_json(silent=True)
        if not request_json or "calls" not in request_json:
            return jsonify({"errorMessage": "Invalid request format"}), 400

        calls = request_json["calls"]
        replies = []
        for call in calls:
            ip = call[0]
            if ip is None:
                replies.append(None)
                continue

            ip = str(ip).strip()
            dot = ip.indexOf('.') if hasattr(ip, 'indexOf') else ip.find('.')
            if dot <= 0:
                replies.append("Unknown")
                continue

            try:
                first_octet_str = ip[:dot]
                first_octet = int(first_octet_str)
                if first_octet == 10 or first_octet == 192 or first_octet == 172:
                    replies.append("Internal")
                elif first_octet < 64:
                    replies.append("Americas")
                elif first_octet < 128:
                    replies.append("EMEA")
                elif first_octet < 192:
                    replies.append("APAC")
                else:
                    replies.append("Other")
            except ValueError:
                replies.append("Unknown")

        return jsonify({"replies": replies})
    except Exception as e:
        return jsonify({"errorMessage": str(e)}), 500
