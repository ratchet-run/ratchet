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
package run.ratchet.store.oracle.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Converts {@link UUID} to a 16-byte big-endian {@code byte[]} for Oracle's {@code RAW(16)} column
 * type, and back.
 *
 * <p>JPA-portable serialization producing identical bytes regardless of provider. The big-endian
 * (most-significant-bits first) layout preserves the UUIDv7 time-ordering the claim range scan
 * relies on, and matches the byte order of {@code ratchet-store-mysql}'s {@code BINARY(16)}
 * converter so a job row is byte-identical across the SQL stores. EclipseLink's default passes the
 * 36-character hyphenated form to JDBC, which overflows {@code RAW(16)}; this converter forces the
 * raw byte order on every write so any provider produces consistent storage.
 *
 * <p>Wired into the persistence unit via {@code META-INF/orm-oracle.xml}, applied per-attribute to
 * every UUID field. Native queries bypass {@link AttributeConverter} per JPA spec — for those call
 * sites use {@link #toBytes(UUID)} directly before {@code setParameter}, and read back via {@code
 * RowValues.uuidOrNull}, which decodes the returned {@code byte[]}.
 */
@Converter
public final class UuidRawConverter implements AttributeConverter<UUID, byte[]> {

  public static byte[] toBytes(UUID uuid) {
    if (uuid == null) {
      return null;
    }
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return buf.array();
  }

  public static UUID fromBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    if (bytes.length != 16) {
      throw new IllegalArgumentException(
          "Expected 16-byte UUID column value, got " + bytes.length + " bytes");
    }
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return new UUID(buf.getLong(), buf.getLong());
  }

  @Override
  public byte[] convertToDatabaseColumn(UUID uuid) {
    return toBytes(uuid);
  }

  @Override
  public UUID convertToEntityAttribute(byte[] bytes) {
    return fromBytes(bytes);
  }
}
