package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import run.ratchet.store.migration.SchemaInitializationException;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Boots a MySQL Testcontainer with the bundled {@code mysql-schema.sql} legacy install applied,
 * then runs {@link SchemaMigrator}. The migrator must refuse to baseline implicitly and surface
 * actionable remediation guidance.
 */
class MysqlSchemaMigratorLegacyIT {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final MySQLContainer CONTAINER =
      new MySQLContainer("mysql:8.0")
          .withDatabaseName("ratchet_legacy_it")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("connectionTimeZone", "UTC")
          .withUrlParam("serverTimezone", "UTC")
          .withInitScript("ddl/mysql-schema.sql");

  @BeforeAll
  static void start() {
    CONTAINER.start();
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  private static DataSource dataSource() {
    return new JdbcDriverDataSource(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  private static Connection newJdbcConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  private static void resetDatabase() throws Exception {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      s.execute("DROP DATABASE " + CONTAINER.getDatabaseName());
      s.execute("CREATE DATABASE " + CONTAINER.getDatabaseName());
    }

    applyLegacySchema();
    // mysql-schema.sql pre-seeds ratchet_schema_version for clean installs so the auto-migrator
    // does not trip its legacy fail-loud guard. Clear it here to faithfully reproduce a
    // pre-migrator legacy install (populated scheduler_* tables, empty version ledger).
    clearVersionLedger();
  }

  private static void clearVersionLedger() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      s.execute("DELETE FROM ratchet_schema_version");
    }
  }

  private static void applyLegacySchema() throws Exception {
    String sql;
    try (var input =
        MysqlSchemaMigratorLegacyIT.class
            .getClassLoader()
            .getResourceAsStream("ddl/mysql-schema.sql")) {
      if (input == null) {
        throw new IOException("Could not load ddl/mysql-schema.sql");
      }
      sql = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }

    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      for (String statement : splitStatements(sql)) {
        s.execute(statement);
      }
    }
  }

  private static List<String> splitStatements(String sql) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean singleQuoted = false;
    boolean doubleQuoted = false;
    boolean lineComment = false;
    boolean blockComment = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';

      if (lineComment) {
        current.append(c);
        if (c == '\n') {
          lineComment = false;
        }
        continue;
      }
      if (blockComment) {
        current.append(c);
        if (c == '*' && next == '/') {
          current.append(next);
          i++;
          blockComment = false;
        }
        continue;
      }
      if (singleQuoted) {
        current.append(c);
        if (c == '\'' && next == '\'') {
          current.append(next);
          i++;
        } else if (c == '\'') {
          singleQuoted = false;
        }
        continue;
      }
      if (doubleQuoted) {
        current.append(c);
        if (c == '"' && next == '"') {
          current.append(next);
          i++;
        } else if (c == '"') {
          doubleQuoted = false;
        }
        continue;
      }

      if (c == '-' && next == '-') {
        current.append(c).append(next);
        i++;
        lineComment = true;
      } else if (c == '/' && next == '*') {
        current.append(c).append(next);
        i++;
        blockComment = true;
      } else if (c == '\'') {
        current.append(c);
        singleQuoted = true;
      } else if (c == '"') {
        current.append(c);
        doubleQuoted = true;
      } else if (c == ';') {
        addStatement(statements, current);
      } else {
        current.append(c);
      }
    }

    addStatement(statements, current);
    return statements;
  }

  private static void addStatement(List<String> statements, StringBuilder current) {
    String statement = current.toString().trim();
    if (!statement.isEmpty()) {
      statements.add(statement);
    }
    current.setLength(0);
  }

  private static void seedSchemaVersionLedger(List<SchemaMigrator.MigrationScript> scripts)
      throws SQLException {
    try (Connection c = newJdbcConnection();
        PreparedStatement s =
            c.prepareStatement(
                "INSERT INTO ratchet_schema_version (version, description, checksum)"
                    + " VALUES (?, ?, ?)")) {
      for (SchemaMigrator.MigrationScript script : scripts) {
        s.setString(1, script.version());
        s.setString(2, script.description());
        s.setString(3, script.checksum());
        s.addBatch();
      }
      s.executeBatch();
    }
  }

  @BeforeEach
  void resetLegacySchema() throws Exception {
    resetDatabase();
  }

  @Test
  void rejectsLegacySchemaWithRemediationGuidance() {
    SchemaInitializationException ex =
        assertThrows(
            SchemaInitializationException.class,
            () -> new SchemaMigrator(dataSource(), "mysql").migrate());
    String message = ex.getMessage();
    assertTrue(message.contains("ratchet_schema_version is empty"), () -> "got: " + message);
    assertTrue(message.contains("seed ratchet_schema_version"), () -> "got: " + message);
    assertTrue(message.contains("ratchet.schema.auto-migrate=false"), () -> "got: " + message);
  }

  @Test
  void acceptsManuallySeededLegacyBaselineThenIsIdempotent() throws Exception {
    SchemaMigrator migrator = new SchemaMigrator(dataSource(), "mysql");
    List<SchemaMigrator.MigrationScript> scripts = migrator.discoverMigrations();
    seedSchemaVersionLedger(scripts);

    SchemaMigrator.MigrationResult first = migrator.migrate();
    assertEquals(scripts.size(), first.skippedCount());
    assertTrue(first.applied().isEmpty(), "seeded legacy baseline should not reapply scripts");
    assertTrue(first.skipped().containsAll(scripts), "seeded baseline should skip all scripts");

    SchemaMigrator.MigrationResult second = new SchemaMigrator(dataSource(), "mysql").migrate();
    assertEquals(scripts.size(), second.skippedCount());
    assertTrue(second.applied().isEmpty(), "remediated legacy baseline should stay idempotent");
    assertTrue(second.skipped().containsAll(scripts), "second run should skip all scripts");
  }
}
