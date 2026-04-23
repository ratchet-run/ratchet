package run.ratchet.store.converter;

import run.ratchet.store.entity.JobPayload;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * JPA {@link AttributeConverter} that converts {@link JobPayload} to/from JSON for database
 * storage.
 *
 * <p>Routes through {@link PayloadSerializerHolder} so the framework's {@link
 * run.ratchet.spi.PayloadSerializer} SPI is the single JSON boundary. JPA converters are
 * not CDI-managed beans, so the holder's static registration pattern is used instead of field
 * injection.
 */
@Converter
public class JobPayloadConverter implements AttributeConverter<JobPayload, String> {

  @Override
  public String convertToDatabaseColumn(JobPayload attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return PayloadSerializerHolder.get().serialize(attribute);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("JobPayload serialization error", e);
    }
  }

  @Override
  public JobPayload convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return PayloadSerializerHolder.get().deserialize(dbData, JobPayload.class);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("JobPayload deserialization error", e);
    }
  }
}
