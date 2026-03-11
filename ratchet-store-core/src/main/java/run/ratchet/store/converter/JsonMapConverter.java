package run.ratchet.store.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, String>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonMapConverter implements AttributeConverter<Map<String, String>, String> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, String>> TYPE_REF = new TypeReference<>() {};

  @Override
  public String convertToDatabaseColumn(Map<String, String> attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(attribute);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize Map<String, String> to JSON", e);
    }
  }

  @Override
  public Map<String, String> convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return MAPPER.readValue(dbData, TYPE_REF);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize Map<String, String> from JSON", e);
    }
  }
}
