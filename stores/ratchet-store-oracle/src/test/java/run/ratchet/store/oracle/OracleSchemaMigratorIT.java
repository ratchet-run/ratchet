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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.oracle.OracleContainer;
import run.ratchet.store.migration.SchemaMigrationException;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.AbstractSchemaMigratorContract;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Runs {@link SchemaMigrator} against a virgin Oracle Testcontainer (no init script). Verifies that
 * the migrator brings the schema up from empty, is idempotent on re-run, and converges when two
 * migrators race — Oracle holds the lock as an {@code EXCLUSIVE} table lock on a dedicated
 * connection because its DDL auto-commits and it has no grant-free session-level advisory lock.
 */
class OracleSchemaMigratorIT extends AbstractSchemaMigratorContract {

  @SuppressWarnings("resource")
  private static final OracleContainer CONTAINER =
      new OracleContainer("gvenzl/oracle-free:slim-faststart")
          .withDatabaseName("ratchet_migrator_it")
          .withUsername("ratchet")
          .withPassword("ratchet")
          // Oracle's SGA needs far more than Docker's default 64 MB /dev/shm; without this the
          // instance OOMs while opening the database (ORA-03113).
          .withSharedMemorySize(2L * 1024 * 1024 * 1024)
          .withStartupTimeout(Duration.ofMinutes(5));

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
  protected String dialect() {
    return "oracle";
  }

  @Override
  protected Connection newJdbcConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  /**
   * Oracle folds unquoted identifiers to upper case in the data dictionary, and {@code
   * DatabaseMetaData#getTables} matches the stored (upper-case) name case-sensitively. The shared
   * contract probes with the lower-case table name, so resolve existence through {@code
   * user_tables} with the name upper-cased instead.
   */
  @Override
  protected boolean tableExists(String name) throws SQLException {
    try (Connection c = newJdbcConnection();
        PreparedStatement s =
            c.prepareStatement("SELECT 1 FROM user_tables WHERE table_name = ?")) {
      s.setString(1, name.toUpperCase(Locale.ROOT));
      try (ResultSet rs = s.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Oracle has no {@code DROP DATABASE}; reset to virgin by dropping every table in the test user's
   * schema (the migrator's own tables plus the {@code ratchet_schema_lock} and {@code
   * ratchet_schema_version} ledgers it creates).
   */
  @Override
  protected void resetDatabase() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = s.executeQuery("SELECT table_name FROM user_tables")) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
      for (String table : tables) {
        s.execute("DROP TABLE \"" + table + "\" CASCADE CONSTRAINTS PURGE");
      }
    }
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

  @Test
  void rejectsPreviouslyAppliedMigrationWithDifferentChecksum() throws Exception {
    resetDatabase();

    SchemaMigrator.MigrationResult first = new SchemaMigrator(dataSource(), "oracle").migrate();
    assertTrue(first.appliedCount() > 0, "expected at least one migration applied");

    String firstVersion = first.applied().get(0).version();
    corruptRecordedChecksum(firstVersion);

    SchemaMigrationException ex =
        assertThrows(
            SchemaMigrationException.class,
            () -> new SchemaMigrator(dataSource(), "oracle").migrate());
    assertTrue(ex.getMessage().contains("Checksum mismatch"), () -> "got: " + ex.getMessage());
    assertTrue(ex.getMessage().contains(firstVersion), () -> "got: " + ex.getMessage());
  }
}
