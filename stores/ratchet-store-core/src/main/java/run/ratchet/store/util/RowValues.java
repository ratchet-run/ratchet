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

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import run.ratchet.api.JobPriority;

/** Shared scalar conversions for native-query row mappers. */
public final class RowValues {

  private RowValues() {}

  public static String stringOrNull(Object value) {
    if (value == null) {
      return null;
    }
    // Oracle returns LOB-backed columns (JSON-as-CLOB, large TEXT) as java.sql.Clob from native
    // queries; toString() would yield an opaque handle, not the content. MySQL/PostgreSQL return
    // these columns as String, so this branch is Oracle-only in practice.
    if (value instanceof Clob clob) {
      return clobToString(clob);
    }
    return value.toString();
  }

  private static String clobToString(Clob clob) {
    // Stream through the character reader rather than clob.getSubString(1, (int) clob.length()):
    // the length is a long, so casting it to int silently overflows for a CLOB larger than
    // Integer.MAX_VALUE characters and would read a truncated or corrupt prefix. Reading in chunks
    // has no size ceiling and avoids a separate length() round-trip on the driver.
    try (Reader reader = clob.getCharacterStream()) {
      StringBuilder sb = new StringBuilder();
      char[] buffer = new char[8192];
      int read;
      while ((read = reader.read(buffer)) != -1) {
        sb.append(buffer, 0, read);
      }
      return sb.toString();
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("Failed to read CLOB column value", e);
    }
    // Deliberately not calling clob.free(): a single hydration may read the same LOB-backed column
    // value more than once (e.g. an encryption framing probe followed by the decrypt read), and
    // freeing the locator after the first read would break the second. Closing the character
    // stream above does not free the locator. The driver reclaims the result-set LOBs when the
    // statement/transaction closes.
  }

  public static Long longOrNull(Object value) {
    return value == null ? null : ((Number) value).longValue();
  }

  /**
   * Coerces a JDBC temporal column value to an {@link Instant}, accepting every type the supported
   * drivers may return ({@link Instant}, {@link Timestamp}, {@link LocalDateTime}, {@link
   * OffsetDateTime}, {@link Date}). {@code LocalDateTime} is interpreted in the JVM default zone.
   * Returns {@code null} for {@code null} or an unrecognized type.
   */
  public static Instant instantOrNull(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant instant) {
      return instant;
    }
    if (value instanceof Timestamp timestamp) {
      return timestamp.toInstant();
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toInstant();
    }
    if (value instanceof LocalDateTime localDateTime) {
      // JPA providers (Hibernate 7+) materialize driver Timestamps into LocalDateTime via
      // Timestamp.toLocalDateTime(), which reads wall time in the JVM default zone regardless of
      // hibernate.jdbc.time_zone. The wall time must be re-interpreted in that same zone to
      // recover the original instant.
      return localDateTime.atZone(ZoneId.systemDefault()).toInstant();
    }
    if (value instanceof Date date) {
      return date.toInstant();
    }
    return null;
  }

  /**
   * Coerces a JDBC boolean column value to a primitive boolean, accepting {@link Boolean}, numeric
   * (non-zero is {@code true}), or string representations. Returns {@code false} for {@code null}.
   */
  public static boolean booleanOrFalse(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof Number number) {
      return number.intValue() != 0;
    }
    return Boolean.parseBoolean(value.toString());
  }

  public static JobPriority safeJobPriority(int persistedCode) {
    return JobPriority.findByPersistedCode(persistedCode).orElse(JobPriority.NORMAL);
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
