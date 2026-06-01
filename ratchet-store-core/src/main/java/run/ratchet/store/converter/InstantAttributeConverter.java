/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Converts {@link Instant} fields through JDBC timestamps for JPA providers without native support.
 *
 * <p>Not auto-applied. Every JPA provider Ratchet targets maps {@link Instant} natively, so this
 * converter only takes effect where a field opts in with {@code @Convert}. Auto-applying it would
 * silently override native {@code TIMESTAMP} handling on every {@link Instant} attribute and route
 * the value through {@link Timestamp}, which is interpreted in the JVM default time zone.
 */
@Converter
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
