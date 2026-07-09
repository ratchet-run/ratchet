/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.util;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jboss.logging.Logger;
import run.ratchet.spi.MaskingContext;
import run.ratchet.store.converter.PayloadSerializerHolder;

/**
 * Utility for masking sensitive fields in payload data before it is returned from a read API. Which
 * fields count as sensitive is decided by the active {@link run.ratchet.spi.PayloadMaskingPolicy},
 * resolved through {@link PayloadMaskingPolicyHolder}.
 */
public final class PayloadMasker {

  private static final Logger log = Logger.getLogger(PayloadMasker.class);
  private static final String MASKED_VALUE = "***REDACTED***";

  private PayloadMasker() {}

  /** Masks sensitive fields in a job payload JSON string; returns null if input is null. */
  public static String maskPayload(String payloadJson) {
    return maskPayload(payloadJson, null);
  }

  /**
   * Context-aware variant of {@link #maskPayload(String)}: the active policy's context-aware
   * overload is consulted per field. A {@code null} context falls back to name-only matching.
   */
  public static String maskPayload(String payloadJson, MaskingContext context) {
    if (payloadJson == null || payloadJson.isEmpty()) {
      return null;
    }

    try (JsonReader reader = Json.createReader(new StringReader(payloadJson))) {
      JsonValue root = reader.readValue();
      if (root.getValueType() == JsonValue.ValueType.OBJECT) {
        return maskObject(root.asJsonObject(), context).build().toString();
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

  /**
   * Masks the values of sensitive entries in a parameter map. Each key is tested against the active
   * {@link run.ratchet.spi.PayloadMaskingPolicy}; matching entries get a redacted value while
   * everything else is copied verbatim. The input map is never modified; a {@code null} or empty
   * map is returned unchanged. Unlike {@link #maskPayload(String)}, matching is purely key-based —
   * parameter values are opaque strings, not nested JSON.
   */
  public static Map<String, String> maskParams(Map<String, String> params) {
    return maskParams(params, null);
  }

  /** Context-aware variant of {@link #maskParams(Map)}. */
  public static Map<String, String> maskParams(Map<String, String> params, MaskingContext context) {
    if (params == null || params.isEmpty()) {
      return params;
    }
    Map<String, String> masked = new LinkedHashMap<>(params.size());
    for (var entry : params.entrySet()) {
      masked.put(
          entry.getKey(),
          isSensitiveField(entry.getKey(), context) ? MASKED_VALUE : entry.getValue());
    }
    return masked;
  }

  private static boolean isSensitiveField(String fieldName, MaskingContext context) {
    return context == null
        ? PayloadMaskingPolicyHolder.get().isSensitiveField(fieldName)
        : PayloadMaskingPolicyHolder.get().isSensitiveField(fieldName, context);
  }

  private static JsonObjectBuilder maskObject(JsonObject object, MaskingContext context) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    for (var entry : object.entrySet()) {
      String key = entry.getKey();
      JsonValue value = entry.getValue();
      if (isSensitiveField(key, context)) {
        builder.add(key, MASKED_VALUE);
      } else if (value.getValueType() == JsonValue.ValueType.OBJECT) {
        builder.add(key, maskObject(value.asJsonObject(), context));
      } else if (value.getValueType() == JsonValue.ValueType.ARRAY) {
        builder.add(key, maskArray(value.asJsonArray(), context));
      } else {
        builder.add(key, value);
      }
    }
    return builder;
  }

  private static JsonArrayBuilder maskArray(JsonArray array, MaskingContext context) {
    JsonArrayBuilder builder = Json.createArrayBuilder();
    for (JsonValue item : array) {
      if (item.getValueType() == JsonValue.ValueType.OBJECT) {
        builder.add(maskObject(item.asJsonObject(), context));
      } else if (item.getValueType() == JsonValue.ValueType.ARRAY) {
        builder.add(maskArray(item.asJsonArray(), context));
      } else {
        builder.add(item);
      }
    }
    return builder;
  }
}
