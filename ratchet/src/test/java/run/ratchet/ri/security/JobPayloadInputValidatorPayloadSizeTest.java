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
package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.PayloadTooLargeException;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.entity.JobPayload;

class JobPayloadInputValidatorPayloadSizeTest {

  private static final int ONE_KIBIBYTE = 1024;

  @AfterEach
  void resetSerializer() {
    new JobPayloadConverter().discardAllPreparedSerializations();
    PayloadSerializerHolder.set(null);
  }

  @Test
  void exactlyAtLimitIsAcceptedAndSerializedOnlyOnceForPersistence() {
    RecordingSerializer serializer = new RecordingSerializer("x".repeat(ONE_KIBIBYTE));
    PayloadSerializerHolder.set(serializer);
    JobPayload payload = payload();
    JobPayloadConverter converter = new JobPayloadConverter();

    String persisted;
    String repeated;
    converter.beginPreparationScope();
    try {
      validator().validateAtCreation(payload);
      persisted = converter.convertToDatabaseColumn(payload);
      repeated = converter.convertToDatabaseColumn(payload);
    } finally {
      converter.endPreparationScope();
    }

    assertEquals("x".repeat(ONE_KIBIBYTE), persisted);
    assertEquals(persisted, repeated);
    assertEquals(1, serializer.serializeCount);
  }

  @Test
  void oneByteOverLimitIsRejectedWithSizeDetails() {
    RecordingSerializer serializer = new RecordingSerializer("x".repeat(ONE_KIBIBYTE + 1));
    PayloadSerializerHolder.set(serializer);

    PayloadTooLargeException exception =
        assertThrows(
            PayloadTooLargeException.class, () -> validator().validateAtCreation(payload()));

    assertEquals(ONE_KIBIBYTE + 1, exception.actualBytes());
    assertEquals(ONE_KIBIBYTE, exception.maxBytes());
    assertEquals(1, serializer.serializeCount);
  }

  @Test
  void multibyteTextIsMeasuredAsUtf8Bytes() {
    RecordingSerializer serializer = new RecordingSerializer("é".repeat(512));
    PayloadSerializerHolder.set(serializer);
    JobPayload payload = payload();

    assertDoesNotThrow(() -> validator().validateAtCreation(payload));

    serializer.serialized = "é".repeat(512) + "a";
    PayloadTooLargeException exception =
        assertThrows(PayloadTooLargeException.class, () -> validator().validateAtCreation(payload));
    assertEquals(ONE_KIBIBYTE + 1, exception.actualBytes());
  }

  @Test
  void rejectedSerializationIsNotRetainedForLaterPersistence() {
    RecordingSerializer serializer = new RecordingSerializer("x".repeat(ONE_KIBIBYTE + 1));
    PayloadSerializerHolder.set(serializer);
    JobPayload payload = payload();

    assertThrows(PayloadTooLargeException.class, () -> validator().validateAtCreation(payload));
    serializer.serialized = "small";

    assertEquals("small", new JobPayloadConverter().convertToDatabaseColumn(payload));
    assertEquals(2, serializer.serializeCount);
  }

  @Test
  void failedRevalidationClearsPreviouslyAcceptedSerialization() {
    RecordingSerializer serializer = new RecordingSerializer("accepted");
    PayloadSerializerHolder.set(serializer);
    JobPayload payload = payload();

    validator().validateAtCreation(payload);
    serializer.serialized = "x".repeat(ONE_KIBIBYTE + 1);
    assertThrows(PayloadTooLargeException.class, () -> validator().validateAtCreation(payload));

    serializer.serialized = "fresh";
    assertEquals("fresh", new JobPayloadConverter().convertToDatabaseColumn(payload));
    assertEquals(3, serializer.serializeCount);
  }

  @Test
  void serializerFailureDoesNotLeavePreparedState() {
    RecordingSerializer serializer = new RecordingSerializer("unused");
    serializer.failure = new IllegalArgumentException("cannot serialize");
    PayloadSerializerHolder.set(serializer);
    JobPayload payload = payload();

    assertThrows(IllegalArgumentException.class, () -> validator().validateAtCreation(payload));
    serializer.failure = null;
    serializer.serialized = "recovered";

    assertEquals("recovered", new JobPayloadConverter().convertToDatabaseColumn(payload));
    assertEquals(2, serializer.serializeCount);
  }

  private static JobPayloadInputValidator validator() {
    RatchetOptions options =
        RatchetOptions.builder().payload(payload -> payload.maxPayloadKb(1)).build();
    return new JobPayloadInputValidator(options);
  }

  private static JobPayload payload() {
    return new JobPayload(
        Target.class.getName(), "accept", "(Ljava/lang/String;)V", true, List.of("value"));
  }

  public static final class Target {

    private Target() {}

    public static void accept(String value) {}
  }

  private static final class RecordingSerializer implements PayloadSerializer {

    private String serialized;
    private RuntimeException failure;
    private int serializeCount;

    private RecordingSerializer(String serialized) {
      this.serialized = serialized;
    }

    @Override
    public String serialize(Object payload) {
      serializeCount++;
      if (failure != null) {
        throw failure;
      }
      return serialized;
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      throw new UnsupportedOperationException();
    }
  }
}
