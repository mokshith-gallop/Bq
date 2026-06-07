import unittest
import json
import re
import hashlib

def js_normalize_country(country):
    """Python implementation matching our BigQuery JS UDF exactly"""
    if country is None:
        return None
    s = str(country).strip().lower()
    if s in ("usa", "us", "united states", "united states of america", "u.s.a."):
        return "US"
    elif s in ("uk", "gb", "united kingdom", "great britain"):
        return "GB"
    elif s in ("de", "germany", "deutschland"):
        return "DE"
    elif s in ("fr", "france"):
        return "FR"
    elif s in ("it", "italy", "italia"):
        return "IT"
    elif s in ("ca", "canada"):
        return "CA"
    else:
        if len(s) == 2:
            return s.upper()
        return "XX"

def js_hash_customer_email(email):
    """Python implementation matching our BigQuery JS UDF and Hive HashCustomerEmail"""
    if email is None:
        return None
    clean = str(email).strip().lower()
    return hashlib.sha256(clean.encode('utf-8')).hexdigest()

def js_parse_legacy_sku(sku):
    """Python implementation matching our BigQuery JS UDF and Hive ParseLegacySku"""
    if sku is None:
        return None
    s = str(sku).strip()
    match = re.match(r"^([A-Z]{2})-(\d{3,5})-(\d{1,3})$", s)
    if not match:
        return "UNKNOWN|0|0"
    return f"{match.group(1)}|{match.group(2)}|{match.group(3)}"


class TestJsUdfs(unittest.TestCase):
    def test_normalize_country(self):
        self.assertEqual(js_normalize_country("United States"), "US")
        self.assertEqual(js_normalize_country("usa"), "US")
        self.assertEqual(js_normalize_country("united kingdom"), "GB")
        self.assertEqual(js_normalize_country("FR"), "FR")
        self.assertEqual(js_normalize_country("germany"), "DE")
        self.assertEqual(js_normalize_country("india"), "XX")
        self.assertEqual(js_normalize_country("in"), "IN")
        self.assertIsNone(js_normalize_country(None))

    def test_hash_customer_email(self):
        email = "a@example.com"
        expected = hashlib.sha256(email.encode('utf-8')).hexdigest()
        self.assertEqual(js_hash_customer_email(email), expected)
        self.assertEqual(js_hash_customer_email("  A@EXAMPLE.COM  "), expected)
        self.assertIsNone(js_hash_customer_email(None))

    def test_parse_legacy_sku(self):
        self.assertEqual(js_parse_legacy_sku("AP-1234-09"), "AP|1234|09")
        self.assertEqual(js_parse_legacy_sku("XY-12345-1"), "XY|12345|1")
        self.assertEqual(js_parse_legacy_sku("invalid"), "UNKNOWN|0|0")
        self.assertEqual(js_parse_legacy_sku("AB-12-34"), "UNKNOWN|0|0")
        self.assertIsNone(js_parse_legacy_sku(None))
