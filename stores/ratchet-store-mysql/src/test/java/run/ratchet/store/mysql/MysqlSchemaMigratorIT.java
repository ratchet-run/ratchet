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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrationException;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.AbstractSchemaMigratorContract;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Runs {@link SchemaMigrator} against a virgin MySQL Testcontainer (no init script). Verifies that
 * the migrator brings the schema up from empty, is idempotent on re-run, and converges under the
 * advisory {@code GET_LOCK} when two threads race.
 */
class MysqlSchemaMigratorIT extends AbstractSchemaMigratorContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final MySQLContainer CONTAINER =
      new MySQLContainer("mysql:8.0.36")
          .withDatabaseName("ratchet_migrator_it")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("connectionTimeZone", "UTC")
          .withUrlParam("serverTimezone", "UTC");

  @BeforeAll
  static void start() {
    CONTAINER.start();
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  @Override
  protected DataSource dataSource() {
    return new JdbcDriverDataSource(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @Override
  protected SchemaMigrationDialect dialect() {
    return new MysqlSchemaMigrationDialect();
  }

  private void corruptRecordedChecksum(String version) throws SQLException {
    try (Connection c = newJdbcConnection();
        PreparedStatement s =
            c.prepareStatement(
                "UPDATE ratchet_schema_version SET checksum = ? WHERE version = ?")) {
      s.setString(1, "not-the-current-checksum");
      s.setString(2, version);
      s.executeUpdate();
    }
  }

  @Override
  protected Connection newJdbcConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @Override
  protected void resetDatabase() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      s.execute("DROP DATABASE " + CONTAINER.getDatabaseName());
      s.execute("CREATE DATABASE " + CONTAINER.getDatabaseName());
    }
  }

  @Test
  void rejectsPreviouslyAppliedMigrationWithDifferentChecksum() throws Exception {
    resetDatabase();

    SchemaMigrator.MigrationResult first =
        new SchemaMigrator(dataSource(), new MysqlSchemaMigrationDialect()).migrate();
    assertTrue(first.appliedCount() > 0, "expected at least one migration applied");

    String firstVersion = first.applied().get(0).version();
    corruptRecordedChecksum(firstVersion);

    SchemaMigrationException ex =
        assertThrows(
            SchemaMigrationException.class,
            () -> new SchemaMigrator(dataSource(), new MysqlSchemaMigrationDialect()).migrate());
    assertTrue(ex.getMessage().contains("Checksum mismatch"), () -> "got: " + ex.getMessage());
    assertTrue(ex.getMessage().contains(firstVersion), () -> "got: " + ex.getMessage());
  }
}
