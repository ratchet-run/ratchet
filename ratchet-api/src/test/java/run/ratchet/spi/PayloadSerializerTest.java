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
package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Type;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadSerializerTest {
  private final PayloadSerializer serializer =
      new PayloadSerializer() {
        @Override
        public String serialize(Object value) {
          return String.valueOf(value);
        }

        @Override
        public <T> T deserialize(String json, Class<T> type) {
          return type.cast(json);
        }
      };

  @Test
  void typeOverloadDelegatesClasses() {
    assertEquals("value", serializer.deserialize("value", (Type) String.class));
  }

  @Test
  void genericTypesRequireOverrideExceptForEmptyInput() throws Exception {
    Type type = getClass().getDeclaredMethod("fixture", List.class).getGenericParameterTypes()[0];
    IllegalArgumentException failure =
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize("[]", type));
    assertTrue(failure.getMessage().contains(type.getTypeName()));
    assertTrue(failure.getMessage().contains("override"));
    assertNull(serializer.deserialize(null, type));
    assertNull(serializer.deserialize("", type));
  }

  static void fixture(List<Long> values) {}
}
