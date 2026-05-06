package run.ratchet.store.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.tck.store.AbstractSchemaConformanceContract;
import run.ratchet.tck.store.schema.DialectTypeMapper;

/**
 * PostgreSQL conformance test for the canonical Ratchet schema. Owns its own Testcontainer (with
 * reuse enabled) so the conformance check is independent of the JPA fixture used by the rest of the
 * TCK suite.
 */
class PostgresqlSchemaConformanceContractTest extends AbstractSchemaConformanceContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_schema")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withInitScript("ddl/postgresql-schema.sql")
          .withReuse(true);

  static {
    CONTAINER.start();
  }

  @Override
  protected Connection openConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @Override
  protected DialectTypeMapper mapper() {
    return new PostgresqlDialectMapper();
  }
}
