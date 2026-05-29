package run.ratchet.ri.testutil;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import run.ratchet.spi.PayloadSerializer;

/**
 * JSON-B-backed {@link PayloadSerializer} implementation for unit tests. Allows tests to obtain a
 * realistic serializer without depending on the CDI-managed {@code JsonbPayloadSerializer} bean.
 */
public final class JsonbTestPayloadSerializer implements PayloadSerializer {

  private final Jsonb jsonb = JsonbBuilder.create();

  @Override
  public String serialize(Object payload) {
    if (payload == null) {
      return null;
    }
    try {
      return jsonb.toJson(payload);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B serialization error for " + payload.getClass().getName(), e);
    }
  }

  @Override
  public <T> T deserialize(String json, Class<T> type) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return jsonb.fromJson(json, type);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B deserialization error for " + (type == null ? "null" : type.getName()), e);
    }
  }
}
