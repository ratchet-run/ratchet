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
package run.ratchet.store.sqlserver;

import jakarta.enterprise.context.ApplicationScoped;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrationException;

/**
 * SQL Server schema migration dialect.
 *
 * <p>The migration lock is a session-scoped {@code sp_getapplock}. Because {@code @LockOwner =
 * 'Session'} ties the lock to the connection rather than a transaction, it survives the migrator's
 * per-script commits without a dedicated lock connection (see {@link
 * #usesDedicatedLockConnection()}). The version-record upsert is a {@code MERGE}.
 */
@ApplicationScoped
public class SqlserverSchemaMigrationDialect implements SchemaMigrationDialect {

  private static final String LOCK_NAME = "ratchet_schema_migration";
  // sp_getapplock @LockTimeout is in milliseconds; a second migrator that cannot acquire the lock
  // within this window fails loudly.
  private static final int LOCK_TIMEOUT_MILLIS = 30000;

  @Override
  public String id() {
    return "sqlserver";
  }

  @Override
  public String createVersionTableSql() {
    // SQL Server has no CREATE TABLE IF NOT EXISTS; guard with OBJECT_ID. Single batch, no internal
    // semicolon, so it runs as one JDBC statement. The statement runs under the migration lock, so
    // there is no concurrent-create race to handle.
    return """
        IF OBJECT_ID(N'ratchet_schema_version', N'U') IS NULL
        CREATE TABLE ratchet_schema_version
        (
            version     VARCHAR(20)  NOT NULL,
            applied_at  DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
            description VARCHAR(200) NOT NULL,
            checksum    VARCHAR(64),
            CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
        )\
        """;
  }

  @Override
  public String recordVersionSql() {
    // T-SQL requires a MERGE statement to be terminated with a semicolon.
    return "MERGE ratchet_schema_version AS tgt"
        + " USING (VALUES (?, ?, ?)) AS src(version, description, checksum)"
        + " ON tgt.version = src.version"
        + " WHEN MATCHED THEN UPDATE SET description = src.description,"
        + " checksum = src.checksum"
        + " WHEN NOT MATCHED THEN INSERT (version, description, checksum)"
        + " VALUES (src.version, src.description, src.checksum);";
  }

  @Override
  public boolean usesDedicatedLockConnection() {
    return false;
  }

  @Override
  public void acquireLock(Connection connection) throws SQLException {
    // sp_getapplock returns >= 0 on grant (0 immediate, 1 after wait), < 0 on failure. A
    // Session-owned lock is held for the life of the connection, so it survives the migrator's
    // per-script commits without a dedicated lock connection.
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "DECLARE @r int;"
                    + " EXEC @r = sp_getapplock @Resource = '"
                    + LOCK_NAME
                    + "', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = "
                    + LOCK_TIMEOUT_MILLIS
                    + ";"
                    + " SELECT @r;")) {
      if (!resultSet.next() || resultSet.getInt(1) < 0) {
        throw new SchemaMigrationException("Timed out acquiring SQL Server schema migration lock");
      }
    }
  }

  @Override
  public void releaseLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "EXEC sp_releaseapplock @Resource = '" + LOCK_NAME + "', @LockOwner = 'Session'");
    }
  }
}
