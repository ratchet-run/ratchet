package run.ratchet.coordinator.infinispan;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonWriter;
import jakarta.json.stream.JsonParsingException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

/**
 * JSON-P encoder/decoder for the Infinispan coordinator's cache-value envelope. Identical wire
 * shape to the PostgreSQL and JMS coordinators so operators see the same JSON across transports.
 */
final class InfinispanNotifyPayloadCodec {

  static final int CURRENT_VERSION = 1;

  String encode(NotifyPayload payload) {
    Objects.requireNonNull(payload, "payload");
    JsonObjectBuilder builder =
        Json.createObjectBuilder()
            .add("v", payload.version())
            .add("node", payload.node().value())
            .add("prio", payload.priority().name());
    if (payload.cid() != null) {
      builder.add("cid", payload.cid());
    }
    JsonObject obj = builder.build();
    StringWriter sw = new StringWriter(96);
    try (JsonWriter writer = Json.createWriter(sw)) {
      writer.writeObject(obj);
    }
    return sw.toString();
  }

  NotifyPayload decode(String json) {
    if (json == null || json.isBlank()) {
      throw new DecodeException("empty notify payload");
    }
    JsonObject obj;
    try (JsonReader reader = Json.createReader(new StringReader(json))) {
      JsonValue root = reader.readValue();
      if (root.getValueType() != JsonValue.ValueType.OBJECT) {
        throw new DecodeException("notify payload root is not a JSON object");
      }
      obj = root.asJsonObject();
    } catch (JsonParsingException e) {
      throw new DecodeException("malformed JSON in notify payload", e);
    }

    int version = readVersion(obj);
    if (version != CURRENT_VERSION) {
      throw new DecodeException(
          "unsupported notify envelope version "
              + version
              + " (this build speaks v"
              + CURRENT_VERSION
              + ")");
    }

    String node = readRequiredString(obj, "node");
    String prio = readRequiredString(obj, "prio");
    JobPriority priority;
    try {
      priority = JobPriority.valueOf(prio);
    } catch (IllegalArgumentException e) {
      throw new DecodeException("unknown JobPriority '" + prio + "' in notify payload", e);
    }

    NodeIdentity identity;
    try {
      identity = new NodeIdentity(node);
    } catch (RuntimeException e) {
      throw new DecodeException("invalid NodeIdentity '" + node + "' in notify payload", e);
    }
    String cid = readOptionalString(obj, "cid");
    return new NotifyPayload(version, identity, priority, cid);
  }

  private static int readVersion(JsonObject obj) {
    JsonValue v = obj.get("v");
    if (v == null) {
      throw new DecodeException("notify payload missing required field 'v'");
    }
    if (v.getValueType() != JsonValue.ValueType.NUMBER) {
      throw new DecodeException("notify payload field 'v' is not a number: " + v.getValueType());
    }
    return obj.getInt("v");
  }

  private static String readRequiredString(JsonObject obj, String field) {
    JsonValue v = obj.get(field);
    if (v == null) {
      throw new DecodeException("notify payload missing required field '" + field + "'");
    }
    if (v.getValueType() != JsonValue.ValueType.STRING) {
      throw new DecodeException(
          "notify payload field '" + field + "' is not a string: " + v.getValueType());
    }
    return ((JsonString) v).getString();
  }

  /** Optional forward-compat string field — absent or non-string returns null. */
  private static String readOptionalString(JsonObject obj, String field) {
    JsonValue v = obj.get(field);
    if (v == null || v.getValueType() != JsonValue.ValueType.STRING) {
      return null;
    }
    return ((JsonString) v).getString();
  }

  /**
   * Decoded envelope passed between the cache listener and dispatch path. {@code cid} is an
   * optional correlation identifier — present on freshly-published envelopes via {@link #current},
   * {@code null} on pre-correlation-id wire formats.
   */
  record NotifyPayload(int version, NodeIdentity node, JobPriority priority, String cid) {

    NotifyPayload {
      Objects.requireNonNull(node, "node");
      Objects.requireNonNull(priority, "priority");
    }

    static NotifyPayload current(NodeIdentity node, JobPriority priority) {
      return new NotifyPayload(CURRENT_VERSION, node, priority, UUID.randomUUID().toString());
    }
  }

  /**
   * Thrown when the wire payload cannot be decoded. Callers surface as transport_failure(parse).
   */
  static final class DecodeException extends RuntimeException {
    DecodeException(String message) {
      super(message);
    }

    DecodeException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
