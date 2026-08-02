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
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

/**
 * JPA {@link AttributeConverter} that converts {@link JobPayload} to/from JSON for database
 * storage.
 *
 * <p>Routes through {@link PayloadSerializerHolder} so the framework's {@link PayloadSerializer}
 * SPI is the single JSON boundary. JPA converters are not CDI-managed beans, so the holder's static
 * registration pattern is used instead of field injection.
 *
 * <p>This converter performs serialization only. Payload-args encryption is applied a layer up, in
 * the row mappers and document mapper, which alone see the whole row and can supply the job id and
 * surface that the {@code byte[]} AEAD seam binds each ciphertext to. A JPA converter only ever
 * receives the value, never the row, so it cannot carry that context.
 */
@Converter
public class JobPayloadConverter implements AttributeConverter<JobPayload, String> {

  /** Starts a synchronous submission scope on the current thread. Scopes may be nested. */
  public void beginPreparationScope() {
    JobPayloadSerialization.beginPreparationScope();
  }

  /** Ends the current synchronous submission scope and releases every staged payload in it. */
  public void endPreparationScope() {
    JobPayloadSerialization.endPreparationScope();
  }

  /**
   * Stages the exact JSON accepted by creation-time validation for the synchronous persistence call
   * on this thread.
   *
   * <p>The creation service surrounds submission with {@link #beginPreparationScope()} and {@link
   * #endPreparationScope()}. Identity keys keep equal-but-distinct payloads independent within a
   * batch.
   */
  public void prepareForPersistence(JobPayload payload, String serialized) {
    JobPayloadSerialization.prepareForPersistence(payload, serialized);
  }

  /** Clears a creation-time serialization staged in the current submission scope. */
  public void discardPreparedSerialization(JobPayload payload) {
    JobPayloadSerialization.discardPreparedSerialization(payload);
  }

  /** Clears every creation-time serialization staged on the current thread. */
  public void discardAllPreparedSerializations() {
    JobPayloadSerialization.discardAllPreparedSerializations();
  }

  @Override
  public String convertToDatabaseColumn(JobPayload attribute) {
    return JobPayloadSerialization.serialize(attribute);
  }

  @Override
  public JobPayload convertToEntityAttribute(String dbData) {
    return JobPayloadSerialization.deserialize(dbData);
  }
}
