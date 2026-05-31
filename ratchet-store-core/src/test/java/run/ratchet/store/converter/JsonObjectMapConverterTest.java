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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadSerializer;

class JsonObjectMapConverterTest {

  private final JsonObjectMapConverter converter = new JsonObjectMapConverter();

  @AfterEach
  void reset() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void roundtrip_preservesMixedTypes() {
    Map<String, Object> original = Map.of("name", "Alice", "age", 30, "active", true);
    String json = converter.convertToDatabaseColumn(original);
    Map<String, Object> restored = converter.convertToEntityAttribute(json);

    assertEquals("Alice", restored.get("name"));
    assertEquals(new BigDecimal("30"), restored.get("age"));
    assertEquals(true, restored.get("active"));
  }

  @Test
  void roundtrip_preservesNestedStructures() {
    Map<String, Object> original =
        Map.of("user", Map.of("name", "Alice", "roles", List.of("admin", "operator"), "score", 99));

    Map<String, Object> restored =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(original));

    assertEquals(
        Map.of(
            "user",
            Map.of(
                "name",
                "Alice",
                "roles",
                List.of("admin", "operator"),
                "score",
                new BigDecimal("99"))),
        restored);
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
    assertThrows(
        IllegalArgumentException.class, () -> converter.convertToEntityAttribute("not json"));
  }

  @Test
  void ignoresInstalledPayloadSerializer() {
    PayloadSerializerHolder.set(new ThrowingSerializer());

    String json = converter.convertToDatabaseColumn(Map.of("name", "Alice"));
    Map<String, Object> restored = converter.convertToEntityAttribute(json);

    assertEquals("Alice", restored.get("name"));
  }

  static final class ThrowingSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) {
      throw new IllegalArgumentException("holder should not be used");
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      throw new IllegalArgumentException("holder should not be used");
    }
  }
}
