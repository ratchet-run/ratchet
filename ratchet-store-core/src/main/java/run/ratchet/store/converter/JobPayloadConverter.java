package run.ratchet.store.converter;

import run.ratchet.store.entity.JobPayload;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * JPA {@link AttributeConverter} that converts {@link JobPayload} to/from JSON for database
 * storage.
 */
@Converter
public class JobPayloadConverter implements AttributeConverter<JobPayload, String> {

  private static final Jsonb JSONB = JsonbBuilder.create();

  @Override
  public String convertToDatabaseColumn(JobPayload attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return JSONB.toJson(attribute);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("JobPayload serialization error", e);
    }
  }

  @Override
  public JobPayload convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return JSONB.fromJson(dbData, JobPayload.class);
    } catch (JsonbException e) {
      throw new IllegalArgumentException("JobPayload deserialization error", e);
    }
  }
}
