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
 * <p>A single {@link Jsonb} instance is reused across all calls. Jakarta JSON Binding 3.0 does not
 * mandate thread-safety for {@code Jsonb}, so access is serialized through a {@link ThreadLocal} to
 * avoid contention on the shared instance while still avoiding per-call {@code
 * JsonbBuilder.create()} overhead (which involves ServiceLoader resolution).
 */
@Converter
public class JsonObjectMapConverter extends AbstractJsonAttributeConverter<Map<String, Object>> {

  // ThreadLocal avoids per-call JsonbBuilder.create() (ServiceLoader + config) while
  // remaining safe for providers that do not guarantee Jsonb thread-safety.
  private static final ThreadLocal<Jsonb> JSONB = ThreadLocal.withInitial(JsonbBuilder::create);

  public String convertToDatabaseColumn(Map<String, Object> attribute) {
    return super.convertToDatabaseColumn(attribute);
  }

  @Override
  protected String serialize(Map<String, Object> attribute) {
    return JSONB.get().toJson(attribute);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, Object> deserialize(String dbData) {
    return (Map<String, Object>) JSONB.get().fromJson(dbData, Map.class);
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
