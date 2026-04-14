package run.ratchet.testsuite.infra;

import java.util.logging.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * JUnit 5 extension that starts a Testcontainers database before all tests.
 *
 * <p>The database type is determined by the {@code ratchet.test.db.type} system property (defaults
 * to "mysql"). The container is started once and shared across all test classes via the JUnit
 * {@link ExtensionContext.Store} with GLOBAL namespace.
 */
public class JdbcContainerExtension
    implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private static final Logger log = Logger.getLogger(JdbcContainerExtension.class.getName());

  private static final String STORE_KEY = "ratchet-db-container";

  private static volatile JdbcDatabaseContainer<?> container;
  private static volatile JdbcDatabaseConfig config;
  private static volatile boolean started = false;

  public static JdbcDatabaseConfig getConfig() {
    if (config == null) {
      throw new IllegalStateException(
          "JdbcContainerExtension has not been initialized. "
              + "Ensure @ExtendWith(JdbcContainerExtension.class) is present.");
    }
    return config;
  }

  @SuppressWarnings({"resource"})
  private static JdbcDatabaseContainer<?> createContainer(String dbType) {
    return switch (dbType) {
      case "mysql" ->
          new MySQLContainer("mysql:8.0")
              .withDatabaseName("ratchet_test")
              .withUsername("ratchet")
              .withPassword("ratchet")
              .withInitScript("ddl/mysql-schema.sql");
      case "postgresql" ->
          new PostgreSQLContainer("postgres:16")
              .withDatabaseName("ratchet_test")
              .withUsername("ratchet")
              .withPassword("ratchet")
              .withInitScript("ddl/postgresql-schema.sql");
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    if (!"mysql".equals(dbType) && !"postgresql".equals(dbType)) {
      return;
    }

    if (started) {
      return;
    }

    synchronized (JdbcContainerExtension.class) {
      if (started) {
        return;
      }

      // Register for cleanup when the root context closes
      context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).put(STORE_KEY, this);

      log.info("Starting Testcontainers database: " + dbType);

      container = createContainer(dbType);
      container.start();

      // PostgreSQL needs `stringtype=unspecified` so the JDBC driver sends Java Strings as
      // untyped text, letting the server cast them to JSONB on insert. Without this, any JPA
      // mapping of a String field to a JSONB column fails with:
      //   "column X is of type jsonb but expression is of type character varying".
      // This is a pure JDBC-driver feature (not Hibernate, not JPA-provider-specific), which
      // keeps ratchet-store-postgresql pluggable across Hibernate / EclipseLink.
      //
      // Strip any existing query-string (testcontainers may add `?loggerLevel=OFF`) before
      // appending — the URL is interpolated into arquillian.xml, and a literal `&` inside an
      // XML attribute value is parsed as an entity reference and breaks deployment.
      String jdbcUrl = container.getJdbcUrl();
      if ("postgresql".equals(dbType)) {
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart >= 0) {
          jdbcUrl = jdbcUrl.substring(0, queryStart);
        }
        jdbcUrl += "?stringtype=unspecified";
      }

      config =
          new JdbcDatabaseConfig(
              jdbcUrl,
              container.getUsername(),
              container.getPassword(),
              container.getDriverClassName(),
              dbType);

      // Set system properties for downstream consumers (datasource config, etc.)
      System.setProperty("ratchet.test.db.url", config.url());
      System.setProperty("ratchet.test.db.username", config.username());
      System.setProperty("ratchet.test.db.password", config.password());
      System.setProperty("ratchet.test.db.driver", config.driverClass());

      // WildFly driver name must match the JDBC driver registered in standalone.xml
      String driverName =
          switch (dbType) {
            case "mysql" -> "mysql";
            case "postgresql" -> "postgresql";
            default -> "h2";
          };
      System.setProperty("ratchet.test.db.driver.name", driverName);

      started = true;
      log.info("Database container ready: " + config.url());
    }
  }

  @Override
  public void close() {
    if (container != null && container.isRunning()) {
      container.stop();
      log.info("Database container stopped");
    }
  }
}
