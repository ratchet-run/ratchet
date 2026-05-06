package run.ratchet.store.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testcontainers.mysql.MySQLContainer;
import run.ratchet.tck.store.AbstractSchemaConformanceContract;
import run.ratchet.tck.store.schema.DialectTypeMapper;

/**
 * MySQL conformance test for the canonical Ratchet schema. Owns its own Testcontainer so the
 * conformance check is independent of the JPA fixture used by the rest of the TCK; the container
 * uses {@code withReuse(true)} so it is shared across test classes when Testcontainers reuse is
 * enabled.
 */
class MysqlSchemaConformanceContractTest extends AbstractSchemaConformanceContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final MySQLContainer CONTAINER =
      new MySQLContainer("mysql:8.0")
          .withDatabaseName("ratchet_schema")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withInitScript("ddl/mysql-schema.sql")
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
    return new MysqlDialectMapper();
  }
}
