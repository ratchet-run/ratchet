package run.ratchet.store.mysql.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Converts {@link UUID} to a 16-byte big-endian {@code byte[]} for MySQL's {@code BINARY(16)}
 * column type, and back.
 *
 * <p>JPA-portable serialization producing identical bytes regardless of provider — Hibernate
 * already maps UUID to BINARY(16) via its built-in UUID type handler, but EclipseLink's default
 * passes the 36-character hyphenated form to JDBC, which overflows BINARY(16) and triggers MySQL
 * error 1406 "Data too long for column". This converter forces the standard byte order
 * (most-significant-bits first) on every write so any provider produces consistent storage.
 *
 * <p>Wired into the persistence unit via {@code META-INF/orm-mysql.xml}, applied per-attribute to
 * every UUID field. Native queries bypass {@link AttributeConverter} per JPA spec — for those call
 * sites use {@link #toBytes(UUID)} directly before {@code setParameter}.
 *
 * <p>Standard byte order matches Java's {@link UUID} serialization. The MySQL operator-facing view
 * {@code vw_jobs} reads via {@code BIN_TO_UUID(col)} (no swap flag), so reads round-trip.
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
