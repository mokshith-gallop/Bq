-- BigQuery DDL for com.acme.udf.HashCustomerEmail
-- Native JavaScript UDF: hash_customer_email(email STRING)
-- Materialized inside the 'udfs' dataset.
--
-- Location-agnostic template (using PROJECT_ID and DATASET)

CREATE OR REPLACE FUNCTION `${PROJECT_ID}.${DATASET}.hash_customer_email`(email STRING)
RETURNS STRING
DETERMINISTIC
LANGUAGE js
AS r"""
  if (email === null || email === undefined) return null;
  var clean = email.trim().toLowerCase();
  
  // SHA-256 implementation in pure JS (since crypto is not natively available in sandboxed JS UDF)
  // Or we can use a built-in SQL optimization/wrapper around SHA256() in BQ where possible,
  // but to match the JS UDF target type:
  function sha256(ascii) {
    function rightRotate(value, amount) {
      return (value>>>amount) | (value<<(32-amount));
    };
    
    var mathPow = Math.pow;
    var maxWord = mathPow(2, 32);
    var lengthProperty = 'length'
    var i, j; // Used as a counter across the whole file
    var result = ''

    var words = [];
    var asciiLength = ascii[lengthProperty]*8;
    
    var hash = sha256.h = sha256.h || [];
    var k = sha256.k = sha256.k || [];
    var primeCounter = k[lengthProperty];

    var isPrime = {};
    for (var candidate = 2; primeCounter < 64; candidate++) {
      if (!isPrime[candidate]) {
        for (i = 0; i < 313; i += candidate) {
          isPrime[i] = candidate;
        }
        hash[primeCounter] = (mathPow(candidate, .5)*maxWord)|0;
        k[primeCounter++] = (mathPow(candidate, 1/3)*maxWord)|0;
      }
    }
    
    ascii += '\x80' // Append Ʀ' bit (also known as 0x80 in octets)
    while (ascii[lengthProperty] % 64 - 56) ascii += '\x00' // Pad with zeroes
    for (i = 0; i < ascii[lengthProperty]; i++) {
      j = ascii.charCodeAt(i);
      if (j >> 8) return null; // ASCII check: only accept standard single-byte chars
      words[i>>2] |= j << (24 - (i % 4)*8);
    }
    words[words[lengthProperty]] = ((asciiLength/maxWord)|0);
    words[words[lengthProperty]] = (asciiLength|0);
    
    // process each chunk
    for (j = 0; j < words[lengthProperty];) {
      var w = words.slice(j, j += 16); // The next 16 words
      var oldHash = hash.slice(0);
      
      hash = hash.slice(0);
      
      for (i = 0; i < 64; i++) {
        var wItem = w[i];
        if (i >= 16) {
          wItem = w[i] = (
            rightRotate(w[i-2], 17) ^ rightRotate(w[i-2], 19) ^ (w[i-2]>>>10)
          ) + w[i-7] + (
            rightRotate(w[i-15], 7) ^ rightRotate(w[i-15], 18) ^ (w[i-15]>>>3)
          ) + w[i-16] | 0;
        }
        
        var temp1 = hash[7] + (rightRotate(hash[4], 6) ^ rightRotate(hash[4], 11) ^ rightRotate(hash[4], 25)) +
          ((hash[4] & hash[5]) ^ (~hash[4] & hash[6])) + k[i] + wItem;
        var temp2 = (rightRotate(hash[0], 2) ^ rightRotate(hash[0], 13) ^ rightRotate(hash[0], 22)) +
          ((hash[0] & hash[1]) ^ (hash[0] & hash[2]) ^ (hash[1] & hash[2]));
        
        hash = [(temp1 + temp2)|0].concat(hash);
        hash[4] = (hash[4] + temp1)|0;
        hash[8] = 0;
        hash.pop();
      }
      
      for (i = 0; i < 8; i++) {
        hash[i] = (hash[i] + oldHash[i])|0;
      }
    }
    
    for (i = 0; i < 8; i++) {
      for (j = 3; j + 1; j--) {
        var b = (hash[i]>>(j*8))&255;
        result += ((b < 16) ? '0' : '') + b.toString(16);
      }
    }
    return result;
  };
  return sha256(clean);
""";
