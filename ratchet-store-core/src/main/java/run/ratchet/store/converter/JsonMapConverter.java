package run.ratchet.store.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, String>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonMapConverter implements AttributeConverter<Map<String, String>, String> {

  @Override
  public String convertToDatabaseColumn(Map<String, String> attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return PayloadSerializerHolder.get().serialize(attribute);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("JSON map serialization error", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return (Map<String, String>) PayloadSerializerHolder.get().deserialize(dbData, Map.class);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("JSON map deserialization error", e);
    }
  }
}
