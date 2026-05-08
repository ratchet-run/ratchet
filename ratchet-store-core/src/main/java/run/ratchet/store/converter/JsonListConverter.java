package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.Converter;
import java.util.List;

/**
 * JPA {@link AttributeConverter} that converts {@code List<Object>} to/from JSON for database
 * storage.
 */
@Converter
public class JsonListConverter extends AbstractJsonAttributeConverter<List<Object>> {

  private static final Jsonb JSONB = JsonbBuilder.create();

  @Override
  protected String serialize(List<Object> attribute) {
    return JSONB.toJson(attribute);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<Object> deserialize(String dbData) {
    return (List<Object>) JSONB.fromJson(dbData, List.class);
  }

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return JsonbException.class;
  }

  @Override
  protected String serializationErrorMessage() {
    return "JSON list serialization error";
  }

  @Override
  protected String deserializationErrorMessage() {
    return "JSON list deserialization error";
  }
}
