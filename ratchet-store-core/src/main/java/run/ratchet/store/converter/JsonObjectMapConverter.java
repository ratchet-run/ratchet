package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, Object>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonObjectMapConverter extends AbstractJsonAttributeConverter<Map<String, Object>> {

  private static final Jsonb JSONB = JsonbBuilder.create();

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
