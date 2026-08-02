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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;

class RowValuesTest {

  @Test
  void instantOrNullCoercesEverySupportedTemporalType() {
    Instant expected = Instant.parse("2026-05-12T12:00:00Z");

    assertNull(RowValues.instantOrNull(null));
    assertEquals(expected, RowValues.instantOrNull(expected));
    assertEquals(expected, RowValues.instantOrNull(Timestamp.from(expected)));
    assertEquals(expected, RowValues.instantOrNull(expected.atOffset(ZoneOffset.UTC)));
    assertEquals(expected, RowValues.instantOrNull(Date.from(expected)));
  }

  @Test
  void instantOrNullInterpretsLocalDateTimeInTheJvmDefaultZone() {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));

      // May 12 is EDT (UTC-4).
      assertEquals(
          Instant.parse("2026-05-12T16:00:00Z"),
          RowValues.instantOrNull(LocalDateTime.parse("2026-05-12T12:00:00")));

      // January 12 is EST (UTC-5), documenting that the offset shifts with DST.
      assertEquals(
          Instant.parse("2026-01-12T17:00:00Z"),
          RowValues.instantOrNull(LocalDateTime.parse("2026-01-12T12:00:00")));
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void instantOrNullReturnsNullForUnsupportedType() {
    assertNull(RowValues.instantOrNull("2026-05-12T12:00:00Z"));
  }

  @Test
  void booleanOrFalseCoercesEverySupportedSourceType() {
    assertFalse(RowValues.booleanOrFalse(null));
    assertTrue(RowValues.booleanOrFalse(Boolean.TRUE));
    assertFalse(RowValues.booleanOrFalse(0));
    assertTrue(RowValues.booleanOrFalse(1));
    assertTrue(RowValues.booleanOrFalse("true"));
  }

  @Test
  void safeJobPriorityReturnsNormalForUnknownPersistedCodes() {
    assertEquals(JobPriority.NORMAL, RowValues.safeJobPriority(-1));
    assertEquals(JobPriority.NORMAL, RowValues.safeJobPriority(99));
  }

  @Test
  void safeJobPriorityReturnsMappedPersistedCode() {
    assertEquals(
        JobPriority.CRITICAL, RowValues.safeJobPriority(JobPriority.CRITICAL.persistedCode()));
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
