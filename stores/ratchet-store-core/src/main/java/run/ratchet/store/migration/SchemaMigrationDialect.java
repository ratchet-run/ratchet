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
package run.ratchet.store.migration;

import java.sql.Connection;
import java.sql.SQLException;
import run.ratchet.api.Incubating;

/**
 * Per-vendor strategy for the dialect-specific parts of {@link SchemaMigrator}.
 *
 * <p>The migrator engine owns everything portable — classpath script discovery, checksum
 * validation, statement splitting, the apply loop, and the {@code ratchet_schema_version} ledger.
 * Everything that differs by database lives behind this interface: the migration lock, the version
 * ledger DDL, and the version-record upsert. A store that wants to participate in Ratchet schema
 * auto-migration supplies one implementation; the engine never switches on a database product name.
 *
 * @apiNote This is a store-implementor SPI, not an application-facing API. Implementations may be
 *     stateless and are used from a single migration thread at a time per connection.
 */
@Incubating
public interface SchemaMigrationDialect {

  /**
   * Canonical dialect id, lower-case ({@code "mysql"}, {@code "postgresql"}, {@code "oracle"}).
   *
   * <p>The schema-migration lifecycle hook matches this id against {@code
   * RATCHET_SCHEMA_MIGRATION_DIALECT} to choose one dialect when several store modules are
   * deployed; the engine no longer infers a dialect from JDBC product metadata.
   *
   * @return the canonical id; never {@code null} or blank
   */
  String id();

  /**
   * Returns a single executable statement that creates {@code ratchet_schema_version} if it does
   * not already exist — the engine runs it before every migration and does not catch already-exists
   * errors, so the statement must be idempotent on its own. The table must expose {@code version},
   * {@code applied_at}, {@code description}, and a nullable {@code checksum} column with {@code
   * version} as the primary key.
   *
   * @return a single executable statement
   */
  String createVersionTableSql();

  /**
   * Returns the parameterized upsert that records an applied migration in {@code
   * ratchet_schema_version}. The three positional parameters are, in order, {@code version}, {@code
   * description}, and {@code checksum}; an existing row for the same version must have its
   * description and checksum overwritten.
   *
   * @return a single executable SQL statement with three positional parameters
   */
  String recordVersionSql();

  /**
   * Whether the migration lock must be held on a dedicated connection separate from the one that
   * runs the migration DDL.
   *
   * <p>Most dialects hold a session-level advisory lock on the migration connection itself. A
   * dialect whose DDL auto-commits (which would drop a transactional lock) returns {@code true},
   * and the engine opens a second connection that runs no DDL to hold the lock for the migration
   * window.
   *
   * @return {@code true} to lock on a dedicated connection, {@code false} to lock on the migration
   *     connection
   */
  boolean usesDedicatedLockConnection();

  /**
   * Acquires the cluster-wide migration lock on the supplied connection, blocking until it is held
   * or failing loudly when the dialect's wait window elapses.
   *
   * @param connection the connection that holds the lock — the dedicated lock connection when
   *     {@link #usesDedicatedLockConnection()} is {@code true}, otherwise the migration connection
   * @throws SQLException if the lock cannot be acquired
   */
  void acquireLock(Connection connection) throws SQLException;

  /**
   * Releases the migration lock previously acquired on the supplied connection.
   *
   * @param connection the same connection passed to {@link #acquireLock(Connection)}
   * @throws SQLException if the lock cannot be released
   */
  void releaseLock(Connection connection) throws SQLException;
}
