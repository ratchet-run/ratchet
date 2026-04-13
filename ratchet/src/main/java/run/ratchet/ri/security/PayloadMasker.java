package run.ratchet.ri.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import run.ratchet.store.util.ObjectMapperFactory;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Masks sensitive fields (passwords, tokens, PII) in job payload JSON before API/log exposure. The
 * original payload in the database is never modified.
 */
public class PayloadMasker {

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
          "apikey",
          "api_key",
          "apisecret",
          "api_secret",
          "accesskey",
          "access_key",
          "accessToken",
          "access_token",
          "refreshToken",
          "refresh_token",
          "credential",
          "credentials",
          "auth",
          "authorization",
          "privatekey",
          "private_key",
          "privateKey",
          "ssn",
          "socialSecurityNumber",
          "creditcard",
          "credit_card",
          "cardNumber",
          "card_number",
          "cvv",
          "cvc",
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
      log.warnf("Failed to mask payload JSON, returning masked value: %s", e.getMessage());
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
      log.warnf("Failed to serialize payload for masking: %s", e.getMessage());
      return MASKED_VALUE;
    }
  }

  private static boolean isSensitiveField(String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String lowerFieldName = fieldName.toLowerCase();
    return SENSITIVE_FIELDS.stream()
        .anyMatch(pattern -> lowerFieldName.contains(pattern.toLowerCase()));
  }

  private static void maskObject(ObjectNode node) {
    node.fieldNames()
        .forEachRemaining(
            fieldName -> {
              JsonNode fieldValue = node.get(fieldName);

              if (isSensitiveField(fieldName)) {
                node.put(fieldName, MASKED_VALUE);
              } else if (fieldValue.isObject()) {
                maskObject((ObjectNode) fieldValue);
              } else if (fieldValue.isArray()) {
                for (int i = 0; i < fieldValue.size(); i++) {
                  JsonNode element = fieldValue.get(i);
                  if (element.isObject()) {
                    maskObject((ObjectNode) element);
                  }
                }
              }
            });
  }
}
