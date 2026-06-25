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
package run.ratchet.store.oracle;

import jakarta.enterprise.context.ApplicationScoped;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrationException;

/**
 * Oracle schema migration dialect.
 *
 * <p>Oracle has no grant-free session-level advisory lock and its DDL auto-commits, so the
 * migration lock is held as an {@code EXCLUSIVE} table lock on a dedicated connection (see {@link
 * #usesDedicatedLockConnection()}). The version-record upsert is a {@code MERGE}.
 */
@ApplicationScoped
public class OracleSchemaMigrationDialect implements SchemaMigrationDialect {

  // WAIT N is in seconds; a second migrator that cannot acquire the lock within this window fails
  // loudly.
  private static final int LOCK_WAIT_SECONDS = 120;
  private static final int ORA_NAME_ALREADY_USED = 955;
  private static final int ORA_RESOURCE_BUSY = 54;

  @Override
  public String id() {
    return "oracle";
  }

  @Override
  public String createVersionTableSql() {
    return """
        CREATE TABLE IF NOT EXISTS ratchet_schema_version
        (
            version     VARCHAR2(20)  NOT NULL,
            applied_at  TIMESTAMP(6)  DEFAULT SYSTIMESTAMP NOT NULL,
            description VARCHAR2(200) NOT NULL,
            checksum    VARCHAR2(64),
            CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
        )\
        """;
  }

  @Override
  public String recordVersionSql() {
    return "MERGE INTO ratchet_schema_version t"
        + " USING (SELECT ? AS version, ? AS description, ? AS checksum FROM dual) s"
        + " ON (t.version = s.version)"
        + " WHEN MATCHED THEN UPDATE SET t.description = s.description,"
        + " t.checksum = s.checksum"
        + " WHEN NOT MATCHED THEN INSERT (version, description, checksum)"
        + " VALUES (s.version, s.description, s.checksum)";
  }

  @Override
  public boolean usesDedicatedLockConnection() {
    return true;
  }

  /**
   * Acquires the Oracle migration lock on a dedicated connection.
   *
   * <p>Oracle DDL auto-commits, which would release any transactional lock held on the migration
   * connection itself, and {@code DBMS_LOCK} requires an EXECUTE grant that managed users often
   * lack. Instead a tiny dedicated {@code ratchet_schema_lock} table is locked {@code IN EXCLUSIVE
   * MODE} on a second connection that never runs DDL, so the lock survives the migration's
   * per-script commits. The lock table is separate from {@code ratchet_schema_version} on purpose:
   * an EXCLUSIVE lock on the ledger would block the migration connection's own ledger writes.
   */
  @Override
  public void acquireLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS ratchet_schema_lock (lock_name VARCHAR2(128) NOT NULL,"
              + " CONSTRAINT pk_ratchet_schema_lock PRIMARY KEY (lock_name))");
    } catch (SQLException e) {
      // ORA-00955: a concurrent migrator created the lock table first. Any other error is fatal.
      if (e.getErrorCode() != ORA_NAME_ALREADY_USED) {
        throw e;
      }
    }
    connection.setAutoCommit(false);
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "LOCK TABLE ratchet_schema_lock IN EXCLUSIVE MODE WAIT " + LOCK_WAIT_SECONDS);
    } catch (SQLException e) {
      if (e.getErrorCode() == ORA_RESOURCE_BUSY) {
        throw new SchemaMigrationException(
            "Timed out after "
                + LOCK_WAIT_SECONDS
                + "s acquiring the Oracle schema migration lock; another migrator held it longer"
                + " than the wait window.",
            e);
      }
      throw e;
    }
  }

  @Override
  public void releaseLock(Connection connection) throws SQLException {
    // Commit releases the EXCLUSIVE table lock; the dedicated lock connection is closed by the
    // engine immediately afterward.
    connection.commit();
  }
}
