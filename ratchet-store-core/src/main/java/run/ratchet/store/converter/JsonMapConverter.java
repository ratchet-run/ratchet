package run.ratchet.store.converter;

import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, String>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonMapConverter extends AbstractJsonAttributeConverter<Map<String, String>> {

  @Override
  protected String serialize(Map<String, String> attribute) {
    return PayloadSerializerHolder.get().serialize(attribute);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, String> deserialize(String dbData) {
    return (Map<String, String>) PayloadSerializerHolder.get().deserialize(dbData, Map.class);
  }

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return IllegalArgumentException.class;
  }

  @Override
  protected String serializationErrorMessage() {
    return "JSON map serialization error";
  }

  @Override
  protected String deserializationErrorMessage() {
    return "JSON map deserialization error";
  }
}
