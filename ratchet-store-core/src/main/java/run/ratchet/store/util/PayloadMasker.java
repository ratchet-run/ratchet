package run.ratchet.store.util;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.util.Locale;
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
          "apikey",
          "api_key",
          "apisecret",
          "api_secret",
          "accesskey",
          "access_key",
          "accesstoken",
          "access_token",
          "refreshtoken",
          "refresh_token",
          "auth",
          "authorization",
          "credential",
          "credentials",
          "privatekey",
          "private_key",
          "ssn",
          "socialsecuritynumber",
          "social_security_number",
          "creditcard",
          "credit_card",
          "cardnumber",
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
      log.warn("Payload masking error, redacting entire payload", e);
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
      log.warn("Payload serialization error, redacting entire payload", e);
      return MASKED_VALUE;
    }
  }

  private static boolean isSensitiveField(String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String[] tokens = tokenize(fieldName);
    if (tokens.length == 0) {
      return false;
    }
    for (String marker : SENSITIVE_FIELDS) {
      String[] markerTokens = tokenize(marker);
      if (containsSubsequence(tokens, markerTokens)) {
        return true;
      }
    }
    return false;
  }

  // Split camelCase + snake_case into lowercase word tokens so substring matches
  // do not produce false positives ("spinner" does not contain a "pin" token) while
  // still flagging compound forms ("privateKeyPem" tokenizes to [private, key, pem]).
  private static String[] tokenize(String fieldName) {
    String spaced = fieldName.replaceAll("([a-z0-9])([A-Z])", "$1 $2");
    return spaced.toLowerCase(Locale.ROOT).split("[^a-z0-9]+");
  }

  private static boolean containsSubsequence(String[] haystack, String[] needle) {
    if (needle.length == 0 || needle.length > haystack.length) {
      return false;
    }
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      boolean match = true;
      for (int j = 0; j < needle.length; j++) {
        if (!haystack[i + j].equals(needle[j])) {
          match = false;
          break;
        }
      }
      if (match) {
        return true;
      }
    }
    return false;
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
