package run.ratchet.store.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Converts {@link Instant} fields through JDBC timestamps for JPA providers without native support.
 */
@Converter(autoApply = true)
public class InstantAttributeConverter implements AttributeConverter<Instant, Timestamp> {

  @Override
  public Timestamp convertToDatabaseColumn(Instant attribute) {
    return attribute == null ? null : Timestamp.from(attribute);
  }

  @Override
  public Instant convertToEntityAttribute(Timestamp dbData) {
    return dbData == null ? null : dbData.toInstant();
  }
}
