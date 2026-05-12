package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PayloadMaskerTest {

  @Test
  void maskPayload_nullOrEmptyInput_returnsNull() {
    assertNull(PayloadMasker.maskPayload((String) null));
    assertNull(PayloadMasker.maskPayload(""));
  }

  @Test
  void maskPayload_masksNestedSensitiveFields() {
    String json =
        """
        {
          "username": "alice",
          "config": {
            "apiKey": "api-secret",
            "endpoint": "https://example.com"
          },
          "accounts": [
            {"refreshToken": "refresh-123"},
            {"metadata": {"privateKeyPem": "-----BEGIN PRIVATE KEY-----"}}
          ]
        }
        """;

    String masked = PayloadMasker.maskPayload(json);

    assertTrue(masked.contains("\"username\":\"alice\""));
    assertTrue(masked.contains("\"apiKey\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"refreshToken\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"privateKeyPem\":\"***REDACTED***\""));
    assertFalse(masked.contains("api-secret"));
    assertFalse(masked.contains("refresh-123"));
    assertFalse(masked.contains("-----BEGIN PRIVATE KEY-----"));
  }

  @Test
  void maskPayload_invalidJson_returnsRedactedValue() {
    assertEquals("***REDACTED***", PayloadMasker.maskPayload("not-valid-json{{{"));
  }

  @Test
  void maskPayload_arrayAndScalarRootsPassThroughUnchanged() {
    assertEquals("[{\"password\":\"x\"}]", PayloadMasker.maskPayload("[{\"password\":\"x\"}]"));
    assertEquals("42", PayloadMasker.maskPayload("42"));
  }

  @Test
  void maskPayload_objectOverloadMasksSerializedObjects() {
    String masked = PayloadMasker.maskPayload(Map.of("password", "secret", "username", "alice"));

    assertTrue(masked.contains("\"password\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"username\":\"alice\""));
    assertFalse(masked.contains("secret"));
  }

  @Test
  void maskPayload_objectOverloadSerializationFailure_returnsRedactedValue() {
    assertEquals("***REDACTED***", PayloadMasker.maskPayload(new ThrowingPayload()));
  }

  @Test
  void maskPayload_usesLocaleRootForSensitiveFieldMatching() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));

      String masked = PayloadMasker.maskPayload("{\"APIKey\":\"secret\"}");

      assertTrue(masked.contains("\"APIKey\":\"***REDACTED***\""));
      assertFalse(masked.contains("secret"));
    } finally {
      Locale.setDefault(previous);
    }
  }

  public static class ThrowingPayload {
    public String getValue() throws IOException {
      throw new IOException("cannot serialize");
    }
  }
}
