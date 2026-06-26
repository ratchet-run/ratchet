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
package run.ratchet.store.sqlserver.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Converts {@link UUID} to a 16-byte big-endian {@code byte[]} for SQL Server's {@code BINARY(16)}
 * column type, and back.
 *
 * <p>The SQL Server store deliberately stores UUIDs as {@code BINARY(16)} holding the canonical
 * big-endian bytes rather than the native {@code UNIQUEIDENTIFIER} type. {@code UNIQUEIDENTIFIER}
 * uses .NET-Guid mixed-endian storage, which EclipseLink 5.0 (GlassFish 8 / Jakarta EE 11 RI) reads
 * byte-swapped from native queries — claimed job ids then no longer match their rows and every job
 * stalls PENDING. Raw bytes are provider-independent, so every JPA provider decodes them
 * identically.
 *
 * <p>Wired into the persistence unit via {@code META-INF/orm-sqlserver.xml}, applied per-attribute
 * to every UUID field. Native queries bypass {@link AttributeConverter} per JPA spec — for those
 * call sites use {@link #toBytes(UUID)} directly before {@code setParameter}. Reads round-trip
 * through {@code RowValues.uuidOrNull}, which decodes the same big-endian byte order.
 */
@Converter
public final class UuidByteArrayConverter implements AttributeConverter<UUID, byte[]> {

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
