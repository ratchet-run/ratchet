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
package run.ratchet.testsuite.infra;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * JUnit 5 extension that starts a shared Testcontainers SQL database before all tests.
 *
 * <p>The database type is determined by the {@code ratchet.test.db.type} system property. The
 * extension starts a container only for {@code mysql}, {@code postgresql}, {@code oracle}, or
 * {@code sqlserver}; other values leave the extension inactive, and {@link #getConfig()} will fail
 * until a SQL-backed run initializes it. The container is started once and shared across all test
 * classes via the JUnit {@link ExtensionContext.Store} with GLOBAL namespace.
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

  private static final String SQLSERVER_DB = "ratchet";
  private static final String SQLSERVER_PASSWORD = "Ratchet!Str0ngPwd";

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
      case "oracle" ->
          new OracleContainer("gvenzl/oracle-free:slim-faststart")
              .withDatabaseName("ratchet_test")
              .withUsername("ratchet")
              .withPassword("ratchet")
              // Oracle's SGA needs far more than Docker's default 64 MB /dev/shm; without this the
              // instance OOMs while opening the database (ORA-03113).
              .withSharedMemorySize(2L * 1024 * 1024 * 1024)
              .withStartupTimeout(Duration.ofMinutes(5))
              .withInitScript("ddl/oracle-schema.sql");
      case "sqlserver" ->
          // No withInitScript: the schema is applied to a dedicated RCSI database after start
          // (see provisionSqlServer). MSSQLServerContainer is sa-only and has no withDatabaseName.
          // withReuse keeps the container warm across local matrix runs (reuse is opt-in via
          // ~/.testcontainers.properties and ignored on ephemeral CI runners), mirroring the
          // store's
          // MssqlContainers; a generous startup timeout absorbs the slow cold start of the x86
          // image
          // under emulation on Apple Silicon, where the default wait expires before SQL Server is
          // up.
          new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-latest")
              .acceptLicense()
              .withPassword(SQLSERVER_PASSWORD)
              .withUrlParam("trustServerCertificate", "true")
              .withStartupTimeout(Duration.ofMinutes(5))
              .withReuse(true);
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    String dbType = System.getProperty("ratchet.test.db.type");
    if (!"mysql".equals(dbType)
        && !"postgresql".equals(dbType)
        && !"oracle".equals(dbType)
        && !"sqlserver".equals(dbType)) {
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
      } else if ("mysql".equals(dbType)) {
        // Force the driver to treat the zone-less DATETIME columns as UTC, matching the database
        // container (whose server zone is UTC). Without this, mysql-connector-j interprets stored
        // timestamps in the JVM-local zone, shifting round-tripped Instants by the JVM's offset
        // from UTC and skewing the NOW(3)-based claim predicates on EclipseLink servers (Payara,
        // GlassFish, OpenLiberty). This URL flows through config.url() into every server's
        // datasource, so a single source keeps them consistent. See MysqlTestFixture for the
        // unit-test analogue. One query param is used deliberately: the URL is interpolated into
        // arquillian.xml attributes, where a literal `&` would be parsed as an XML entity and
        // break deployment.
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart >= 0) {
          jdbcUrl = jdbcUrl.substring(0, queryStart);
        }
        jdbcUrl += "?connectionTimeZone=UTC";
      } else if ("sqlserver".equals(dbType)) {
        // SQL Server's default lock-based READ COMMITTED takes shared read locks (unlike the MVCC
        // engines Ratchet targets), so concurrent claim/cancel paths deadlock. Provision a
        // dedicated
        // database with READ_COMMITTED_SNAPSHOT (which cannot be set on master) and apply the
        // schema
        // there. SQL Server JDBC URLs use ';' separators, which are XML-safe in arquillian.xml.
        jdbcUrl = provisionSqlServer(container);
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
            case "oracle" -> "oracle";
            case "sqlserver" -> "sqlserver";
            default -> "h2";
          };
      System.setProperty("ratchet.test.db.driver.name", driverName);

      started = true;
      log.info("Database container ready: " + config.url());
    }
  }

  /**
   * Creates the dedicated {@code ratchet} database with {@code READ_COMMITTED_SNAPSHOT} enabled,
   * applies the SQL Server schema there, and returns the JDBC URL targeting it. Mirrors the unit
   * fixture's {@code MssqlContainers}.
   */
  private static String provisionSqlServer(JdbcDatabaseContainer<?> mssql) {
    String master = mssql.getJdbcUrl();
    try (Connection c =
            DriverManager.getConnection(master, mssql.getUsername(), mssql.getPassword());
        Statement s = c.createStatement()) {
      s.execute(
          "IF DB_ID('"
              + SQLSERVER_DB
              + "') IS NOT NULL BEGIN ALTER DATABASE ["
              + SQLSERVER_DB
              + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["
              + SQLSERVER_DB
              + "]; END");
      s.execute("CREATE DATABASE [" + SQLSERVER_DB + "]");
      s.execute("ALTER DATABASE [" + SQLSERVER_DB + "] SET READ_COMMITTED_SNAPSHOT ON");
      s.execute("ALTER DATABASE [" + SQLSERVER_DB + "] SET ALLOW_SNAPSHOT_ISOLATION ON");
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to provision the SQL Server ratchet database", e);
    }
    String ratchetUrl = master + ";databaseName=" + SQLSERVER_DB;
    applySqlServerSchema(ratchetUrl, mssql.getUsername(), mssql.getPassword());
    return ratchetUrl;
  }

  private static void applySqlServerSchema(String url, String user, String password) {
    String schema = stripSqlComments(readSqlServerSchema());
    try (Connection conn = DriverManager.getConnection(url, user, password);
        Statement st = conn.createStatement()) {
      for (String raw : schema.split(";")) {
        String stmt = raw.strip();
        if (!stmt.isBlank()) {
          st.execute(stmt);
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to apply the SQL Server schema", e);
    }
  }

  private static String stripSqlComments(String sql) {
    return Arrays.stream(sql.split("\n"))
        .filter(line -> !line.stripLeading().startsWith("--"))
        .collect(Collectors.joining("\n"));
  }

  private static String readSqlServerSchema() {
    try (InputStream in =
        JdbcContainerExtension.class.getResourceAsStream("/ddl/sqlserver-schema.sql")) {
      if (in == null) {
        throw new IllegalStateException(
            "ddl/sqlserver-schema.sql not found — is the sqlserver profile active?");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
