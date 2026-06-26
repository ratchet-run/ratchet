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
package run.ratchet.store.postgresql;

import jakarta.enterprise.context.ApplicationScoped;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import run.ratchet.store.migration.SchemaMigrationDialect;

/** PostgreSQL schema migration dialect: session-level {@code pg_advisory_lock}. */
@ApplicationScoped
public class PostgresqlSchemaMigrationDialect implements SchemaMigrationDialect {

  private static final long LOCK_KEY = 0x52617463686574L;

  @Override
  public String id() {
    return "postgresql";
  }

  @Override
  public String createVersionTableSql() {
    return """
        CREATE TABLE IF NOT EXISTS ratchet_schema_version
        (
            version VARCHAR(20) NOT NULL,
            applied_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
            description VARCHAR(200) NOT NULL,
            checksum VARCHAR(64),
            CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
        )\
        """;
  }

  @Override
  public String recordVersionSql() {
    return "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)"
        + " ON CONFLICT (version) DO UPDATE SET description = EXCLUDED.description,"
        + " checksum = EXCLUDED.checksum";
  }

  @Override
  public boolean usesDedicatedLockConnection() {
    return false;
  }

  @Override
  public void acquireLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("SELECT pg_advisory_lock(" + LOCK_KEY + ")");
    }
  }

  @Override
  public void releaseLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("SELECT pg_advisory_unlock(" + LOCK_KEY + ")");
    }
  }
}
