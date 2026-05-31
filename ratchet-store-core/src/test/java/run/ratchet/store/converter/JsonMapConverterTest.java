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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadSerializer;

class JsonMapConverterTest {

  private final JsonMapConverter converter = new JsonMapConverter();

  @AfterEach
  void reset() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void roundtrip_preservesEntries() {
    Map<String, String> original = Map.of("key1", "value1", "key2", "value2");
    String json = converter.convertToDatabaseColumn(original);
    Map<String, String> restored = converter.convertToEntityAttribute(json);

    assertEquals("value1", restored.get("key1"));
    assertEquals("value2", restored.get("key2"));
    assertEquals(2, restored.size());
  }

  @Test
  void roundtrip_preservesEmptyMap() {
    Map<String, String> restored =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(Map.of()));

    assertEquals(Map.of(), restored);
  }

  @Test
  void nullAttribute_returnsNull() {
    assertNull(converter.convertToDatabaseColumn(null));
  }

  @Test
  void nullDbData_returnsNull() {
    assertNull(converter.convertToEntityAttribute(null));
  }

  @Test
  void emptyString_returnsNull() {
    assertNull(converter.convertToEntityAttribute(""));
  }

  @Test
  void malformedJson_throwsException() {
    assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute("{bad}"));
  }

  @Test
  void routesThroughInstalledPayloadSerializer() {
    RecordingSerializer recorder = new RecordingSerializer();
    PayloadSerializerHolder.set(recorder);

    String json = converter.convertToDatabaseColumn(Map.of("key", "value"));
    Map<String, String> restored = converter.convertToEntityAttribute(json);

    assertEquals("value", restored.get("key"));
    assertEquals(1, recorder.serializeCount.get());
    assertEquals(1, recorder.deserializeCount.get());
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
}
