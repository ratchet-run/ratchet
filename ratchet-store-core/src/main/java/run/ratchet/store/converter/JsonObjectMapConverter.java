package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, Object>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonObjectMapConverter implements AttributeConverter<Map<String, Object>, String> {

  private static final Jsonb JSONB = JsonbBuilder.create();

  @Override
  public String convertToDatabaseColumn(Map<String, Object> attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return JSONB.toJson(attribute);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("JSON object-map serialization error", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return (Map<String, Object>) JSONB.fromJson(dbData, Map.class);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("JSON object-map deserialization error", e);
    }
  }
}
