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
import run.ratchet.store.entity.JobPayload;

/**
 * JPA {@link AttributeConverter} that converts {@link JobPayload} to/from JSON for database
 * storage.
 *
 * <p>Routes through {@link PayloadSerializerHolder} so the framework's {@link
 * run.ratchet.spi.PayloadSerializer} SPI is the single JSON boundary. JPA converters are not
 * CDI-managed beans, so the holder's static registration pattern is used instead of field
 * injection.
 *
 * <p>This converter performs serialization only. Payload-args encryption is applied a layer up, in
 * the row mappers and document mapper, which alone see the whole row and can supply the job id and
 * surface that the {@code byte[]} AEAD seam binds each ciphertext to. A JPA converter only ever
 * receives the value, never the row, so it cannot carry that context.
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
