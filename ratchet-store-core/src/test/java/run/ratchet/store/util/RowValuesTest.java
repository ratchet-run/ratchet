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
