package run.ratchet.coordinator.common.internal;

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
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.DecodeException;
import run.ratchet.coordinator.common.NotifyPayload;

/**
 * JSON-P encoder/decoder for the wakeup envelope shared by every Ratchet cluster coordinator.
 *
 * <p>Envelope shape (version 2):
 *
 * <pre>{@code
 * {"v":2,"node":"<NodeIdentity.value>","prio":"HIGH|NORMAL|LOW|LOWEST|CRITICAL","target":"platform"}
 * }</pre>
 *
 * <p>The {@code target} field is optional: it carries the originating job's execution target when
 * the wakeup is target-scoped, and is omitted otherwise. Unknown fields are ignored for forward
 * compatibility. Removing or repurposing fields bumps {@code v}; unsupported versions fail decode.
 *
 * @apiNote Framework-internal. This codec defines the wire format used by Ratchet's bundled
 *     coordinator transports; it is not part of the public coordinator SPI and the envelope schema
 *     (including {@link #CURRENT_VERSION}) may change without notice.
 */
public final class NotifyPayloadCodec {

  public static final int CURRENT_VERSION = 2;

  public String encode(NotifyPayload payload) {
    Objects.requireNonNull(payload, "payload");
    JsonObjectBuilder builder =
        Json.createObjectBuilder()
            .add("v", payload.version())
            .add("node", payload.node().value())
            .add("prio", payload.priority().name());
    if (payload.executionTarget() != null) {
      builder.add("target", payload.executionTarget());
    }
    JsonObject obj = builder.build();
    StringWriter sw = new StringWriter(96);
    try (JsonWriter writer = Json.createWriter(sw)) {
      writer.writeObject(obj);
    }
    return sw.toString();
  }

  public NotifyPayload decode(String json) {
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
    String executionTarget = readOptionalString(obj, "target");
    return new NotifyPayload(version, identity, priority, executionTarget);
  }

  private static String readOptionalString(JsonObject obj, String field) {
    JsonValue v = obj.get(field);
    if (v == null || v.getValueType() == JsonValue.ValueType.NULL) {
      return null;
    }
    if (v.getValueType() != JsonValue.ValueType.STRING) {
      throw new DecodeException(
          "notify payload field '" + field + "' is not a string: " + v.getValueType());
    }
    return ((JsonString) v).getString();
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
}
