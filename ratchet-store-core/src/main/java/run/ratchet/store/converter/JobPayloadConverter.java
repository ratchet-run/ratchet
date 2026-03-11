package run.ratchet.store.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.store.entity.JobPayload;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * JPA {@link AttributeConverter} that converts {@link JobPayload} to/from JSON for database
 * storage.
 */
@Converter
public class JobPayloadConverter implements AttributeConverter<JobPayload, String> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String convertToDatabaseColumn(JobPayload attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(attribute);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize JobPayload to JSON", e);
    }
  }

  @Override
  public JobPayload convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return MAPPER.readValue(dbData, JobPayload.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize JobPayload from JSON", e);
    }
  }
}
