package run.ratchet.store.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Set;
import org.jboss.logging.Logger;

/** Utility for masking sensitive fields in serialized payload JSON. */
public final class PayloadMasker {

  private static final Logger log = Logger.getLogger(PayloadMasker.class);
  private static final ObjectMapper MAPPER = ObjectMapperFactory.get();
  private static final String MASKED_VALUE = "***REDACTED***";

  private static final Set<String> SENSITIVE_FIELDS =
      Set.of(
          "password",
          "passwd",
          "pwd",
          "secret",
          "token",
          "apiKey",
          "api_key",
          "apikey",
          "apiSecret",
          "api_secret",
          "accessKey",
          "access_key",
          "accessToken",
          "access_token",
          "refreshToken",
          "refresh_token",
          "auth",
          "authorization",
          "credential",
          "credentials",
          "privateKey",
          "private_key",
          "ssn",
          "socialSecurityNumber",
          "social_security_number",
          "creditCard",
          "credit_card",
          "cardNumber",
          "card_number",
          "cvv",
          "pin");

  private PayloadMasker() {}

  /** Masks sensitive fields in a job payload JSON string; returns null if input is null. */
  public static String maskPayload(String payloadJson) {
    if (payloadJson == null || payloadJson.isEmpty()) {
      return null;
    }

    try {
      JsonNode root = MAPPER.readTree(payloadJson);
      if (root.isObject()) {
        maskObject((ObjectNode) root);
      }
      return MAPPER.writeValueAsString(root);
    } catch (Exception e) {
      log.warnf("Payload masking error, redacting: %s", e.getMessage());
      return MASKED_VALUE;
    }
  }

  /**
   * Serializes {@code payload} to JSON then masks sensitive fields; returns null if input is null.
   */
  public static String maskPayload(Object payload) {
    if (payload == null) {
      return null;
    }

    try {
      String json = MAPPER.writeValueAsString(payload);
      return maskPayload(json);
    } catch (Exception e) {
      log.warnf("Payload serialization error, redacting: %s", e.getMessage());
      return MASKED_VALUE;
    }
  }

  private static boolean isSensitiveField(String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String lowerFieldName = fieldName.toLowerCase();
    return SENSITIVE_FIELDS.stream()
        .anyMatch(sensitive -> lowerFieldName.contains(sensitive.toLowerCase()));
  }

  private static void maskArray(ArrayNode array) {
    for (JsonNode item : array) {
      if (item.isObject()) {
        maskObject((ObjectNode) item);
      } else if (item.isArray()) {
        maskArray((ArrayNode) item);
      }
    }
  }

  private static void maskObject(ObjectNode node) {
    node.fieldNames()
        .forEachRemaining(
            fieldName -> {
              JsonNode child = node.get(fieldName);
              if (isSensitiveField(fieldName)) {
                node.put(fieldName, MASKED_VALUE);
              } else if (child.isObject()) {
                maskObject((ObjectNode) child);
              } else if (child.isArray()) {
                maskArray((ArrayNode) child);
              }
            });
  }
}
