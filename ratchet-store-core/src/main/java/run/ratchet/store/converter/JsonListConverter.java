package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.List;

/**
 * JPA {@link AttributeConverter} that converts {@code List<Object>} to/from JSON for database
 * storage.
 */
@Converter
public class JsonListConverter implements AttributeConverter<List<Object>, String> {

  private static final Jsonb JSONB = JsonbBuilder.create();

  @Override
  public String convertToDatabaseColumn(List<Object> attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return JSONB.toJson(attribute);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("JSON list serialization error", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Object> convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return (List<Object>) JSONB.fromJson(dbData, List.class);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("JSON list deserialization error", e);
    }
  }
}
