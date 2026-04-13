package run.ratchet.ri.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import run.ratchet.store.util.ObjectMapperFactory;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Utility class for masking sensitive data in job payloads before exposing them via API responses.
 *
 * <p>This class is essential for data protection and compliance. Job payloads may contain sensitive
 * information such as authentication tokens, API keys, passwords, or personal data (PII/PHI). When
 * exposing job information through dashboard APIs or logs, this sensitive data must be redacted to
 * prevent information disclosure vulnerabilities.
 *
 * <p>The masking process:
 *
 * <ol>
 *   <li>Parses the JSON payload into a tree structure
 *   <li>Recursively traverses all fields, including nested objects and arrays
 *   <li>Identifies sensitive fields by checking field names against known patterns
 *   <li>Replaces sensitive values with a fixed redaction placeholder
 *   <li>Returns the sanitized JSON for safe display
 * </ol>
 *
 * <p><b>Important:</b> This class is designed for display purposes only. The original unmasked
 * payload is always preserved in the database and used for job execution. Masking only affects API
 * responses and logging output.
 */
public class PayloadMasker {

  private static final Logger log = Logger.getLogger(PayloadMasker.class);

  /**
   * Shared Jackson ObjectMapper instance for JSON parsing and serialization. Configured with
   * JavaTimeModule for correct java.time handling.
   */
  private static final ObjectMapper MAPPER = ObjectMapperFactory.get();

  /** The placeholder string used to replace sensitive values. */
  private static final String MASKED_VALUE = "***REDACTED***";

  /**
   * Set of field name patterns that are considered sensitive and should be masked.
   *
   * <p>These patterns are matched case-insensitively using substring matching.
   */
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

  /** Private constructor to prevent instantiation of this utility class. */
  private PayloadMasker() {
    /* utility class */
  }

  /**
   * Masks sensitive fields in a job payload JSON string.
   *
   * @param payloadJson the JSON string representation of the job payload
   * @return a JSON string with sensitive fields masked, or null if input is null
   */
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
      return "{\"error\":\"Unable to parse payload\",\"masked\":true}";
    }
  }

  /**
   * Masks a job payload object directly.
   *
   * @param payload the job payload object (can be any serializable object)
   * @return a JSON string with sensitive fields masked, or null if input is null
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
      return "{\"error\":\"Unable to serialize payload\",\"masked\":true}";
    }
  }

  /**
   * Checks if a field name matches any sensitive field pattern.
   *
   * @param fieldName the field name to check
   * @return true if the field should be masked, false otherwise
   */
  private static boolean isSensitiveField(String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String lowerFieldName = fieldName.toLowerCase();
    return SENSITIVE_FIELDS.stream()
        .anyMatch(pattern -> lowerFieldName.contains(pattern.toLowerCase()));
  }

  /**
   * Recursively masks sensitive fields in a JSON object node.
   *
   * @param node the object node to mask (modified in place)
   */
  private static void maskObject(ObjectNode node) {
    node.fieldNames()
        .forEachRemaining(
            fieldName -> {
              JsonNode fieldValue = node.get(fieldName);

              if (isSensitiveField(fieldName)) {
                node.put(fieldName, MASKED_VALUE);
                log.debugf("Masked sensitive field: %s", fieldName);
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
