package run.ratchet.store.util;

import java.util.UUID;
import run.ratchet.api.JobPriority;

/** Shared scalar conversions for native-query row mappers. */
public final class RowValues {

  private static final JobPriority[] JOB_PRIORITY_VALUES = JobPriority.values();

  private RowValues() {}

  public static String stringOrNull(Object value) {
    return value == null ? null : value.toString();
  }

  public static Long longOrNull(Object value) {
    return value == null ? null : ((Number) value).longValue();
  }

  public static JobPriority safeJobPriority(int ordinal) {
    if (ordinal < 0 || ordinal >= JOB_PRIORITY_VALUES.length) {
      return JobPriority.NORMAL;
    }
    return JOB_PRIORITY_VALUES[ordinal];
  }

  public static UUID uuidOrNull(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof UUID uuid) {
      return uuid;
    }
    if (value instanceof byte[] bytes) {
      return uuidFromBytes(bytes);
    }
    if (value instanceof CharSequence text) {
      return UUID.fromString(text.toString());
    }
    throw new IllegalArgumentException(
        "Unsupported UUID source type: " + value.getClass().getName());
  }

  private static UUID uuidFromBytes(byte[] bytes) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException("UUID byte array must be 16 bytes, got " + bytes.length);
    }
    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (bytes[i] & 0xff);
    }
    for (int i = 8; i < 16; i++) {
      lsb = (lsb << 8) | (bytes[i] & 0xff);
    }
    return new UUID(msb, lsb);
  }
}
