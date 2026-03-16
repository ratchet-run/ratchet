package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, String>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonMapConverter implements AttributeConverter<Map<String, String>, String> {

  private static final Jsonb JSONB = JsonbBuilder.create();

  @Override
  public String convertToDatabaseColumn(Map<String, String> attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return JSONB.toJson(attribute);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("Failed to serialize Map<String, String> to JSON", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return (Map<String, String>) JSONB.fromJson(dbData, Map.class);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("Failed to deserialize Map<String, String> from JSON", e);
    }
  }
}
