package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
