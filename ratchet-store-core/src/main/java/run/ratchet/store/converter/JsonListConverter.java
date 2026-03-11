package run.ratchet.store.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.List;

/**
 * JPA {@link AttributeConverter} that converts {@code List<Object>} to/from JSON for database
 * storage.
 */
@Converter
public class JsonListConverter implements AttributeConverter<List<Object>, String> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<List<Object>> TYPE_REF = new TypeReference<>() {};

  @Override
  public String convertToDatabaseColumn(List<Object> attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(attribute);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize List<Object> to JSON", e);
    }
  }

  @Override
  public List<Object> convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return MAPPER.readValue(dbData, TYPE_REF);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize List<Object> from JSON", e);
    }
  }
}
