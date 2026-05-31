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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

/**
 * Proves that {@link JobPayloadConverter} (JPA-instantiated, not CDI-managed) routes JSON I/O
 * through the framework's installed {@link PayloadSerializer}. This is the wiring the /dg review
 * asked for: not "is the interface producible", but "does the framework actually call it".
 */
class PayloadSerializerHolderTest {

  @AfterEach
  void reset() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void set_installsDelegate() {
    PayloadSerializer mock = new RecordingSerializer();
    PayloadSerializerHolder.set(mock);
    assertSame(mock, PayloadSerializerHolder.get());
  }

  @Test
  void jobPayloadConverter_routesThroughInstalledSerializer() {
    RecordingSerializer recorder = new RecordingSerializer();
    PayloadSerializerHolder.set(recorder);

    JobPayloadConverter converter = new JobPayloadConverter();
    JobPayload payload = new JobPayload("com.example.Svc", "run", "()V", true, List.of("a", "b"));

    String json = converter.convertToDatabaseColumn(payload);
    assertNotNull(json);
    assertEquals(1, recorder.serializeCount.get());

    JobPayload restored = converter.convertToEntityAttribute(json);
    assertNotNull(restored);
    assertEquals(1, recorder.deserializeCount.get());
    assertEquals(payload.target(), restored.target());
  }

  @Test
  void unset_fallsBackToInternalJsonb() {
    PayloadSerializerHolder.set(null);
    JobPayloadConverter converter = new JobPayloadConverter();
    JobPayload payload = new JobPayload("t", "m", "()V", false, List.of());

    // Fallback path must still work for unit tests / non-CDI environments.
    String json = converter.convertToDatabaseColumn(payload);
    JobPayload back = converter.convertToEntityAttribute(json);
    assertEquals(payload.target(), back.target());
  }

  @Test
  void jobPayloadConverter_wrapsSerializationErrors() {
    PayloadSerializerHolder.set(new ThrowingSerializer());
    JobPayloadConverter converter = new JobPayloadConverter();
    JobPayload payload = new JobPayload("t", "m", "()V", false, List.of());

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> converter.convertToDatabaseColumn(payload));

    assertEquals("JobPayload serialization error", thrown.getMessage());
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
  }

  @Test
  void jobPayloadConverter_wrapsDeserializationErrors() {
    PayloadSerializerHolder.set(new ThrowingSerializer());
    JobPayloadConverter converter = new JobPayloadConverter();

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> converter.convertToEntityAttribute("{}"));

    assertEquals("JobPayload deserialization error", thrown.getMessage());
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
  }

  /** Records framework invocations while delegating JSON via a nested JSON-B call. */
  static final class RecordingSerializer implements PayloadSerializer {

    final AtomicInteger serializeCount = new AtomicInteger();
    final AtomicInteger deserializeCount = new AtomicInteger();
    private final Jsonb jsonb = JsonbBuilder.create();

    @Override
    public String serialize(Object payload) {
      serializeCount.incrementAndGet();
      return payload == null ? null : jsonb.toJson(payload);
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      deserializeCount.incrementAndGet();
      if (json == null || json.isEmpty()) {
        return null;
      }
      return jsonb.fromJson(json, type);
    }
  }

  static final class ThrowingSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) {
      throw new IllegalArgumentException("serialize failed");
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      throw new IllegalArgumentException("deserialize failed");
    }
  }
}
