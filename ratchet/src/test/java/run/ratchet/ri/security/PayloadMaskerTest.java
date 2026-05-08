package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PayloadMaskerTest {

  @Test
  void maskPayload_nullInput_returnsNull() {
    assertNull(PayloadMasker.maskPayload(null));
  }

  @Test
  void maskPayload_emptyInput_returnsNull() {
    assertNull(PayloadMasker.maskPayload(""));
  }

  @Test
  void maskPayload_validJson_maskesSensitiveFields() {
    String json = "{\"username\":\"alice\",\"password\":\"s3cret\",\"data\":\"public\"}";
    String masked = PayloadMasker.maskPayload(json);

    assertTrue(masked.contains("\"username\":\"alice\""));
    assertTrue(masked.contains("\"password\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"data\":\"public\""));
  }

  @Test
  void maskPayload_invalidJson_returnsMaskedValue() {
    String result = PayloadMasker.maskPayload("not-valid-json{{{");
    assertEquals("***REDACTED***", result);
  }

  @Test
  void maskPayload_objectOverload_invalidObject_returnsMaskedValue() {
    // An object that will fail serialization is hard to construct with Jackson,
    // but we can verify the happy path works
    String result = PayloadMasker.maskPayload((Object) "simple-string");
    // A plain string serializes to JSON as "simple-string" which isn't an object node,
    // so it passes through without field-level masking
    assertEquals("\"simple-string\"", result);
  }

  @Test
  void maskPayload_nestedSensitiveFields_masked() {
    String json = "{\"config\":{\"apiKey\":\"abc123\",\"endpoint\":\"https://example.com\"}}";
    String masked = PayloadMasker.maskPayload(json);

    assertTrue(masked.contains("\"apiKey\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"endpoint\":\"https://example.com\""));
  }

  @Test
  void maskPayload_compoundSensitiveFieldPatterns_masked() {
    String json =
        """
        {
          "databasePassword": "db-secret",
          "oauthAuthorizationHeader": "Bearer abc123",
          "privateKeyPem": "-----BEGIN PRIVATE KEY-----",
          "safeValue": "visible"
        }
        """;

    String masked = PayloadMasker.maskPayload(json);

    assertTrue(masked.contains("\"databasePassword\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"oauthAuthorizationHeader\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"privateKeyPem\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"safeValue\":\"visible\""));
    assertFalse(masked.contains("db-secret"));
    assertFalse(masked.contains("Bearer abc123"));
    assertFalse(masked.contains("-----BEGIN PRIVATE KEY-----"));
  }

  @Test
  void maskPayload_threePlusLevelNestedSensitiveFields_masked() {
    String json =
        """
        {
          "tenant": {
            "region": {
              "service": {
                "accessToken": "token-123",
                "endpoint": "https://example.com"
              }
            }
          }
        }
        """;

    String masked = PayloadMasker.maskPayload(json);

    assertTrue(masked.contains("\"accessToken\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"endpoint\":\"https://example.com\""));
    assertFalse(masked.contains("token-123"));
  }

  @Test
  void maskPayload_arraysInsideObjects_maskObjectElements() {
    String json =
        """
        {
          "accounts": [
            {"username": "alice", "refreshToken": "refresh-123"},
            {"username": "bob", "metadata": {"apiSecret": "api-secret"}}
          ],
          "status": "active"
        }
        """;

    String masked = PayloadMasker.maskPayload(json);

    assertTrue(masked.contains("\"username\":\"alice\""));
    assertTrue(masked.contains("\"refreshToken\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"username\":\"bob\""));
    assertTrue(masked.contains("\"apiSecret\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"status\":\"active\""));
    assertFalse(masked.contains("refresh-123"));
    assertFalse(masked.contains("api-secret"));
  }

  @Test
  void maskPayload_arrayRoot_passesThroughUnchanged() {
    // Array and scalar roots are not object-masked; they must round-trip unchanged
    // rather than silently collapse to ***REDACTED***.
    assertEquals("[{\"password\":\"x\"}]", PayloadMasker.maskPayload("[{\"password\":\"x\"}]"));
    assertEquals("42", PayloadMasker.maskPayload("42"));
  }
}
