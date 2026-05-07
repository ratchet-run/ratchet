package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
}
