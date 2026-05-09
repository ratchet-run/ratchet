package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
    assertLegacySchemaRemediationGuidance(ex.getMessage());
  }

  private static void assertLegacySchemaRemediationGuidance(String message) {
    List<String> requiredGuidance =
        List.of(
            "ratchet_schema_version is empty",
            "seed ratchet_schema_version",
            "ratchet.schema.auto-migrate=false");
    List<String> missingGuidance =
        requiredGuidance.stream()
            .filter(fragment -> message == null || !message.contains(fragment))
            .toList();

    assertTrue(
        missingGuidance.isEmpty(),
        () ->
            "Expected legacy-schema remediation guidance to include "
                + missingGuidance
                + ", got: "
                + message);
  }
}
