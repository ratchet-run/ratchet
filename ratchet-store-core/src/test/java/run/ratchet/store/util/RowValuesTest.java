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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;

class RowValuesTest {

  @Test
  void safeJobPriorityReturnsNormalForOutOfRangeOrdinals() {
    assertEquals(JobPriority.NORMAL, RowValues.safeJobPriority(-1));
    assertEquals(JobPriority.NORMAL, RowValues.safeJobPriority(JobPriority.values().length));
  }

  @Test
  void safeJobPriorityReturnsOrdinalValueWhenPresent() {
    assertEquals(JobPriority.CRITICAL, RowValues.safeJobPriority(JobPriority.CRITICAL.ordinal()));
  }

  @Test
  void uuidOrNullConvertsUuidStringsAndByteArrays() {
    UUID id = UUID.fromString("018f7c2a-0000-7000-8000-000000000001");
    byte[] bytes =
        ByteBuffer.allocate(16)
            .putLong(id.getMostSignificantBits())
            .putLong(id.getLeastSignificantBits())
            .array();

    assertNull(RowValues.uuidOrNull(null));
    assertEquals(id, RowValues.uuidOrNull(id));
    assertEquals(id, RowValues.uuidOrNull(id.toString()));
    assertEquals(id, RowValues.uuidOrNull(bytes));
  }

  @Test
  void uuidOrNullRejectsInvalidByteArrayLength() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> RowValues.uuidOrNull(new byte[15]));

    assertEquals("UUID byte array must be 16 bytes, got 15", thrown.getMessage());
  }

  @Test
  void uuidOrNullRejectsNonTextValuesWithTypeName() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> RowValues.uuidOrNull(42));

    assertEquals("Unsupported UUID source type: java.lang.Integer", thrown.getMessage());
  }
}
