package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, Object>} to/from JSON for
 * database storage.
 *
 * <p>A single {@link Jsonb} instance is reused across all calls. Holding one shared instance avoids
 * the per-call {@code JsonbBuilder.create()} overhead (which involves ServiceLoader resolution)
 * without attaching per-thread state to pooled/virtual execution threads, matching the shared
 * fallback instance maintained by {@link PayloadSerializerHolder}.
 */
@Converter
public class JsonObjectMapConverter extends AbstractJsonAttributeConverter<Map<String, Object>> {

  // Single shared instance: avoids per-call JsonbBuilder.create() (ServiceLoader + config) without
  // pinning a never-cleaned Jsonb to each pooled/virtual thread. Held for the JVM lifetime.
  private static final Jsonb JSONB = JsonbBuilder.create();

  public String convertToDatabaseColumn(Map<String, Object> attribute) {
    return super.convertToDatabaseColumn(attribute);
  }

  @Override
  protected String serialize(Map<String, Object> attribute) {
    return JSONB.toJson(attribute);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, Object> deserialize(String dbData) {
    return (Map<String, Object>) JSONB.fromJson(dbData, Map.class);
  }

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return JsonbException.class;
  }

  @Override
  protected String serializationErrorMessage() {
    return "JSON object-map serialization error";
  }

  @Override
  protected String deserializationErrorMessage() {
    return "JSON object-map deserialization error";
  }
}
