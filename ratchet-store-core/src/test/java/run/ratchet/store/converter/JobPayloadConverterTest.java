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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.json.bind.JsonbBuilder;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobPayload;

class JobPayloadConverterTest {

  private final JobPayloadConverter converter = new JobPayloadConverter();

  @Test
  void roundtrip_preservesAllFields() {
    JobPayload original = samplePayload();
    String json = converter.convertToDatabaseColumn(original);
    JobPayload restored = converter.convertToEntityAttribute(json);

    assertEquals(original.target(), restored.target());
    assertEquals(original.method(), restored.method());
    assertEquals(original.methodDescriptor(), restored.methodDescriptor());
    assertEquals(original.isStatic(), restored.isStatic());
    assertEquals(original.args(), restored.args());
  }

  @Test
  void nullAttribute_returnsNullJson() {
    assertNull(converter.convertToDatabaseColumn(null));
  }

  @Test
  void nullDbData_returnsNullPayload() {
    assertNull(converter.convertToEntityAttribute(null));
  }

  @Test
  void emptyString_returnsNull() {
    assertNull(converter.convertToEntityAttribute(""));
  }

  @Test
  void producesValidJson() {
    String json = converter.convertToDatabaseColumn(samplePayload());
    Map<?, ?> parsed = JsonbBuilder.create().fromJson(json, Map.class);

    assertEquals("com.example.MyService", parsed.get("target"));
    assertEquals("process", parsed.get("method"));
    assertEquals("(Ljava/lang/String;)V", parsed.get("methodDescriptor"));
    assertEquals(true, parsed.get("isStatic"));
    assertEquals(List.of("hello"), parsed.get("args"));
  }

  @Test
  void malformedJson_throwsException() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> converter.convertToEntityAttribute("{not valid json"));

    assertEquals("JobPayload deserialization error", thrown.getMessage());
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
  }

  @Test
  void roundtrip_preservesEmptyArgs() {
    JobPayload original = new JobPayload("com.example.MyService", "ping", "()V", true, List.of());

    JobPayload restored =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(original));

    assertEquals(List.of(), restored.args());
  }

  @Test
  void roundtrip_preservesComplexArgs() {
    JobPayload original =
        new JobPayload(
            "com.example.MyService",
            "process",
            "(Ljava/util/Map;)V",
            true,
            List.of(Map.of("nested", List.of("a", "b"), "enabled", true)));

    JobPayload restored =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(original));

    assertEquals(original.args(), restored.args());
  }

  private JobPayload samplePayload() {
    return new JobPayload(
        "com.example.MyService", "process", "(Ljava/lang/String;)V", true, List.of("hello"));
  }
}
