package run.ratchet.tck.store.schema;

/**
 * Logical column types in the canonical Ratchet schema. Dialect mappers translate each value into
 * the set of dialect-specific types that satisfy it (e.g. {@link #INT64} → MySQL {@code BIGINT
 * UNSIGNED} ∪ PostgreSQL {@code bigint}; {@link #UUID} → PostgreSQL {@code uuid}, MySQL {@code
 * BINARY(16)} per Hibernate's default mapping for {@code java.util.UUID}).
 */
public enum LogicalType {
  INT32,
  INT64,
  UUID,
  TEXT,
  CHAR_1,
  TIMESTAMP_TZ,
  BOOLEAN,
  JSON
}
