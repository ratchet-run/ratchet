package run.ratchet.store.util;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.util.Set;
import org.jboss.logging.Logger;
import run.ratchet.store.converter.PayloadSerializerHolder;

/** Utility for masking sensitive fields in serialized payload JSON. */
public final class PayloadMasker {

  private static final Logger log = Logger.getLogger(PayloadMasker.class);
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

    try (JsonReader reader = Json.createReader(new StringReader(payloadJson))) {
      JsonValue root = reader.readValue();
      if (root.getValueType() == JsonValue.ValueType.OBJECT) {
        return maskObject(root.asJsonObject()).build().toString();
      }
      // Array and scalar roots pass through unchanged: field-level masking only applies
      // when the root is an object.
      return root.toString();
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
      String json = PayloadSerializerHolder.get().serialize(payload);
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

  private static JsonObjectBuilder maskObject(JsonObject object) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    for (var entry : object.entrySet()) {
      String key = entry.getKey();
      JsonValue value = entry.getValue();
      if (isSensitiveField(key)) {
        builder.add(key, MASKED_VALUE);
      } else if (value.getValueType() == JsonValue.ValueType.OBJECT) {
        builder.add(key, maskObject(value.asJsonObject()));
      } else if (value.getValueType() == JsonValue.ValueType.ARRAY) {
        builder.add(key, maskArray(value.asJsonArray()));
      } else {
        builder.add(key, value);
      }
    }
    return builder;
  }

  private static JsonArrayBuilder maskArray(JsonArray array) {
    JsonArrayBuilder builder = Json.createArrayBuilder();
    for (JsonValue item : array) {
      if (item.getValueType() == JsonValue.ValueType.OBJECT) {
        builder.add(maskObject(item.asJsonObject()));
      } else if (item.getValueType() == JsonValue.ValueType.ARRAY) {
        builder.add(maskArray(item.asJsonArray()));
      } else {
        builder.add(item);
      }
    }
    return builder;
  }
}
