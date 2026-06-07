import unittest
from unittest.mock import MagicMock
import json
import sys
import os

# Add the functions to python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from functions.lookup_supplier_terms.main import lookup_supplier_terms
from functions.geoip_city.main import geoip_city

class TestCloudFunctions(unittest.TestCase):
    def test_geoip_city(self):
        # Create a mock request object
        mock_request = MagicMock()
        mock_request.get_json.return_value = {
            "requestId": "123",
            "calls": [
                ["10.0.0.1"],
                ["192.168.1.1"],
                ["172.16.0.1"],
                ["8.8.8.8"],
                ["120.25.1.1"],
                [None],
                ["bad_ip"]
            ]
        }
        
        response_data, status_code, headers = geoip_city(mock_request)
        self.assertEqual(status_code, 200)
        self.assertEqual(headers["Content-Type"], "application/json")
        
        body = json.loads(response_data)
        self.assertIn("replies", body)
        self.assertEqual(body["replies"], [
            "Internal",
            "Internal",
            "Internal",
            "Americas",
            "EMEA",
            None,
            "Unknown"
        ])

    def test_lookup_supplier_terms(self):
        mock_request = MagicMock()
        mock_request.get_json.return_value = {
            "requestId": "123",
            "calls": [
                ["SUP-001"],
                ["SUP-002"],
                ["SUP-003"],
                ["SUP-UNKNOWN"],
                [None]
            ]
        }
        
        # Stub the GCS check by using an empty terms_map or failing lazily
        # In main.py, it handles exceptions and falls back to stubs SUP-001, SUP-002, SUP-003
        response_data, status_code, headers = lookup_supplier_terms(mock_request)
        self.assertEqual(status_code, 200)
        self.assertEqual(headers["Content-Type"], "application/json")
        
        body = json.loads(response_data)
        self.assertIn("replies", body)
        self.assertEqual(body["replies"], [
            30,
            45,
            60,
            30,
            30
        ])

if __name__ == "__main__":
    unittest.main()
