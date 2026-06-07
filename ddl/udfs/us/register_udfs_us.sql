-- BigQuery DDL for registering UDFs in the US project (acme-analytics, dataset: udfs)

-- =============================================================================
-- 1. normalize_country (Native JavaScript UDF)
-- =============================================================================
CREATE OR REPLACE FUNCTION `${PROJECT_US}.${DS_UDFS}.normalize_country`(country STRING)
RETURNS STRING
DETERMINISTIC
LANGUAGE js
AS """
  if (country === null) return null;
  var s = country.trim().toLowerCase();
  switch (s) {
    case "usa": case "us": case "united states":
    case "united states of america": case "u.s.a.":
      return "US";
    case "uk": case "gb": case "united kingdom": case "great britain":
      return "GB";
    case "de": case "germany": case "deutschland":
      return "DE";
    case "fr": case "france":
      return "FR";
    case "it": case "italy": case "italia":
      return "IT";
    case "ca": case "canada":
      return "CA";
    default:
      if (s.length === 2) return s.toUpperCase();
      return "XX";
  }
""";

-- =============================================================================
-- 2. hash_customer_email (Native JavaScript UDF)
-- =============================================================================
CREATE OR REPLACE FUNCTION `${PROJECT_US}.${DS_UDFS}.hash_customer_email`(email STRING)
RETURNS STRING
DETERMINISTIC
LANGUAGE js
AS """
  if (email === null) return null;
  try {
    // Basic JS implementation of SHA-256 (noting that BigQuery also supports native SHA256)
    // To ensure full custom logic with trimming and lowercase:
    var clean_email = email.trim().toLowerCase();
    
    // Simple but fully functional SHA-256 inside JS
    // We can also use standard crypto JS implementation, but BQ JS sandbox doesn't load external npm libraries unless referenced via GCS.
    // Instead of complex inline JS SHA-256, we can write standard SHA-256 using native JS methods if available or simple bitwise operations.
    // Alternatively, let's write a standard JS SHA-256 function inline.
    
    function sha256(ascii) {
      function rightRotate(value, amount) {
        return (value>>>amount) | (value<<(32-amount));
      };
      
      var mathPow = Math.pow;
      var maxWord = mathPow(2, 32);
      var lengthProperty = 'length';
      var i, j; // Used as a counter across the whole file
      var result = '';
      var words = [];
      var asciiLength = ascii[lengthProperty];
      var hash = sha256.h = sha256.h || [];
      var k = sha256.k = sha256.k || [];
      var primeCounter = k[lengthProperty];

      var isPrime = {};
      for (var candidate = 2; primeCounter < 64; candidate++) {
        if (!isPrime[candidate]) {
          for (i = 0; i < 313; i += candidate) {
            isPrime[i] = 1;
          }
          hash[primeCounter] = (mathPow(candidate, .5)*maxWord)|0;
          k[primeCounter++] = (mathPow(candidate, 1/3)*maxWord)|0;
        }
      }
      
      ascii += '\\x80';
      while (ascii[lengthProperty] % 64 - 56) ascii += '\\x00';
      for (i = 0; i < ascii[lengthProperty]; i++) {
        j = ascii.charCodeAt(i);
        if (j >> 8) return null; // keep only 8-bit characters
        words[i >> 2] |= j << (24 - (i % 4) * 8);
      }
      words[words[lengthProperty]] = ((asciiLength * 8) / maxWord) | 0;
      words[words[lengthProperty]] = (asciiLength * 8);
      
      for (j = 0; j < words[lengthProperty];) {
        var w = words.slice(j, j += 16);
        var oldHash = hash.slice(0);
        hash = hash.slice(0, 8);
        
        for (i = 0; i < 64; i++) {
          var w16 = w[i - 16], w15 = w[i - 15], w2 = w[i - 2], w7 = w[i - 7];
          var a0 = w[i] = (i < 16) ? w[i] : (
            w[i - 16] +
            (rightRotate(w15, 7) ^ rightRotate(w15, 18) ^ (w15 >>> 3)) +
            w7 +
            (rightRotate(w2, 17) ^ rightRotate(w2, 19) ^ (w2 >>> 10))
          ) | 0;
          
          var a = hash[0], e = hash[4];
          var temp1 = hash[7] +
            (rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25)) +
            ((e & hash[5]) ^ (~e & hash[6])) +
            k[i] + a0;
          var temp2 = (rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22)) +
            ((a & hash[1]) ^ (a & hash[2]) ^ (hash[1] & hash[2]));
          
          hash = [(temp1 + temp2) | 0].concat(hash);
          hash[4] = (hash[4] + temp1) | 0;
        }
        
        for (i = 0; i < 8; i++) {
          hash[i] = (hash[i] + oldHash[i]) | 0;
        }
      }
      
      for (i = 0; i < 8; i++) {
        var byteVal = hash[i];
        if (byteVal < 0) byteVal += 4294967296;
        var hex = byteVal.toString(16);
        while (hex.length < 8) hex = '0' + hex;
        result += hex;
      }
      return result;
    }
    
    return sha256(clean_email);
  } catch (e) {
    return null;
  }
""";

-- =============================================================================
-- 3. parse_legacy_sku (Native JavaScript UDF)
-- =============================================================================
CREATE OR REPLACE FUNCTION `${PROJECT_US}.${DS_UDFS}.parse_legacy_sku`(sku STRING)
RETURNS STRING
DETERMINISTIC
LANGUAGE js
AS """
  if (sku === null) return null;
  var s = sku.trim();
  var regex = /^([A-Z]{2})-(\\\\d{3,5})-(\\\\d{1,3})$/;
  var match = regex.exec(s);
  if (!match) return "UNKNOWN|0|0";
  return match[1] + "|" + match[2] + "|" + match[3];
""";

-- =============================================================================
-- 4. geoip_city (Cloud Functions-backed Remote UDF)
-- =============================================================================
CREATE OR REPLACE FUNCTION `${PROJECT_US}.${DS_UDFS}.geoip_city`(ip STRING)
RETURNS STRING
REMOTE WITH CONNECTION `${PROJECT_US}.us.remote-udfs`
OPTIONS (
  endpoint = 'https://us-central1-acme-analytics.cloudfunctions.net/geoip_city'
);

-- =============================================================================
-- 5. lookup_supplier_terms (Cloud Functions-backed Remote UDF)
-- =============================================================================
CREATE OR REPLACE FUNCTION `${PROJECT_US}.${DS_UDFS}.lookup_supplier_terms`(supplier_id STRING)
RETURNS INT64
REMOTE WITH CONNECTION `${PROJECT_US}.us.remote-udfs`
OPTIONS (
  endpoint = 'https://us-central1-acme-analytics.cloudfunctions.net/lookup_supplier_terms'
);
