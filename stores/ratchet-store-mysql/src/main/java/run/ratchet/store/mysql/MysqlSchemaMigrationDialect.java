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
package run.ratchet.store.mysql;

import jakarta.enterprise.context.ApplicationScoped;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrationException;

/** MySQL/MariaDB schema migration dialect: session-level {@code GET_LOCK} advisory lock. */
@ApplicationScoped
public class MysqlSchemaMigrationDialect implements SchemaMigrationDialect {

  private static final String LOCK_NAME = "ratchet_schema_migration";

  @Override
  public String id() {
    return "mysql";
  }

  @Override
  public String createVersionTableSql() {
    return """
        CREATE TABLE IF NOT EXISTS ratchet_schema_version
        (
            version     VARCHAR(20)  NOT NULL,
            applied_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            description VARCHAR(200) NOT NULL,
            checksum    VARCHAR(64)  NULL,
            PRIMARY KEY (version)
        ) ENGINE = InnoDB
          DEFAULT CHARSET = utf8mb4
          COLLATE = utf8mb4_unicode_ci\
        """;
  }

  @Override
  public String recordVersionSql() {
    return "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)"
        + " ON DUPLICATE KEY UPDATE description = VALUES(description),"
        + " checksum = VALUES(checksum)";
  }

  @Override
  public boolean usesDedicatedLockConnection() {
    return false;
  }

  @Override
  public void acquireLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT GET_LOCK('" + LOCK_NAME + "', 30)")) {
      if (!resultSet.next() || resultSet.getInt(1) != 1) {
        throw new SchemaMigrationException("Timed out acquiring MySQL schema migration lock");
      }
    }
  }

  @Override
  public void releaseLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT RELEASE_LOCK('" + LOCK_NAME + "')")) {
      if (!resultSet.next() || resultSet.getInt(1) != 1) {
        throw new SQLException("Failed to release MySQL schema migration lock");
      }
    }
  }
}
