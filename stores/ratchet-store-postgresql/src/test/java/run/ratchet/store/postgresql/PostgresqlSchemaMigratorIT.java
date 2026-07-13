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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.AbstractSchemaMigratorContract;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Runs {@link SchemaMigrator} against a virgin PostgreSQL Testcontainer (no init script). Verifies
 * that the migrator brings the schema up from empty, is idempotent on re-run, and converges under
 * {@code pg_advisory_lock} when two threads race.
 */
class PostgresqlSchemaMigratorIT extends AbstractSchemaMigratorContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_migrator_it")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("stringtype", "unspecified");

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

  private void installReleasedV001(SchemaMigrator.MigrationScript script) throws Exception {
    try (Connection connection = newJdbcConnection()) {
      executeSqlScript(connection, script.sql());
      try (PreparedStatement statement =
          connection.prepareStatement(dialect().recordVersionSql())) {
        statement.setString(1, script.version());
        statement.setString(2, script.description());
        statement.setString(3, script.checksum());
        statement.executeUpdate();
      }
    }
  }

  private void installConsolidatedSchema() throws Exception {
    String resourceName = "ddl/postgresql-schema.sql";
    try (Connection connection = newJdbcConnection()) {
      executeSqlScript(connection, readClasspathResource(resourceName));
    }
  }

  private static void executeSqlScript(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static String readClasspathResource(String resourceName) throws IOException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try (InputStream input = classLoader.getResourceAsStream(resourceName)) {
      if (input == null) {
        throw new IOException("Missing classpath resource " + resourceName);
      }
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private boolean columnExists(String tableName, String columnName) throws SQLException {
    try (Connection connection = newJdbcConnection();
        ResultSet columns =
            connection.getMetaData().getColumns(null, null, tableName, columnName)) {
      return columns.next();
    }
  }

  private void assertExtensionSchemaExists() throws SQLException {
    assertTrue(tableExists("scheduler_job_properties"));
    assertTrue(tableExists("scheduler_job_extension_state"));
    assertTrue(columnExists("scheduler_job_archive", "properties"));
    assertTrue(columnExists("scheduler_job_archive", "extension_state"));
  }

  private static List<String> versions(List<SchemaMigrator.MigrationScript> scripts) {
    return scripts.stream().map(SchemaMigrator.MigrationScript::version).toList();
  }

  @Override
  protected SchemaMigrationDialect dialect() {
    return new PostgresqlSchemaMigrationDialect();
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
      s.execute("DROP SCHEMA public CASCADE");
      s.execute("CREATE SCHEMA public");
    }
  }

  @Test
  void upgradesReleasedV001LedgerToCurrentSchema() throws Exception {
    resetDatabase();
    SchemaMigrator migrator = newMigrator();
    SchemaMigrator.MigrationScript releasedV001 = migrator.discoverMigrations().get(0);
    assertEquals(
        "de49b983ef0b9110af22f59047a9e91fd1b37efdf7a790241b8297f82c857160",
        releasedV001.checksum());
    installReleasedV001(releasedV001);

    SchemaMigrator.MigrationResult result = migrator.migrate();

    assertEquals(List.of("002", "003", "004", "005", "006"), versions(result.applied()));
    assertEquals(List.of("001"), versions(result.skipped()));
    assertExtensionSchemaExists();
    assertSchemaVersionRowsMatch(migrator.discoverMigrations());
  }

  @Test
  void migratesConsolidatedSchemaWithoutLedgerRows() throws Exception {
    resetDatabase();
    installConsolidatedSchema();
    SchemaMigrator migrator = newMigrator();

    SchemaMigrator.MigrationResult result = migrator.migrate();

    assertEquals(List.of("001", "002", "003", "004", "005", "006"), versions(result.applied()));
    assertEquals(List.of(), result.skipped());
    assertExtensionSchemaExists();
    assertSchemaVersionRowsMatch(migrator.discoverMigrations());
  }
}
