package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.store.migration.SchemaInitializationException;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Boots a PostgreSQL Testcontainer with the bundled {@code postgresql-schema.sql} legacy install
 * applied, then runs {@link SchemaMigrator}. The migrator must refuse to baseline implicitly and
 * surface actionable remediation guidance.
 */
class PostgresqlSchemaMigratorLegacyIT {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_legacy_it")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("stringtype", "unspecified")
          .withInitScript("ddl/postgresql-schema.sql");

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
            () -> new SchemaMigrator(dataSource(), "postgresql").migrate());
    String message = ex.getMessage();
    assertTrue(message.contains("ratchet_schema_version is empty"), () -> "got: " + message);
    assertTrue(message.contains("seed ratchet_schema_version"), () -> "got: " + message);
    assertTrue(message.contains("ratchet.schema.auto-migrate=false"), () -> "got: " + message);
  }
}
