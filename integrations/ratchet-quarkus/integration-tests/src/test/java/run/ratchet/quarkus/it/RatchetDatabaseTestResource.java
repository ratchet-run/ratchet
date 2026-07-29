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
package run.ratchet.quarkus.it;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
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
import java.util.Map;
import java.util.stream.Collectors;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts the database selected by {@code quarkus.datasource.db-kind} with standalone Testcontainers
 * 2.0.5 instead of Quarkus Dev Services.
 *
 * <p>Quarkus 3.20 Dev Services bundles Testcontainers 1.x, whose docker-java client advertises
 * Docker API 1.32. Docker Engine 29 rejects that because its minimum API is 1.40. Standalone
 * Testcontainers 2.0.5 negotiates correctly, so this preserves zero-setup database provisioning
 * while avoiding the Dev Services client-version failure.
 */
public class RatchetDatabaseTestResource implements QuarkusTestResourceLifecycleManager {

  private static final String DB_KIND =
      System.getProperty("quarkus.datasource.db-kind", "postgresql");

  private static final DatabaseFixture FIXTURE = createFixture(DB_KIND);

  static {
    FIXTURE.start();
  }

  @SuppressWarnings("resource")
  private static DatabaseFixture createFixture(String dbKind) {
    if ("mongodb".equals(dbKind)) {
      return new MongoFixture();
    }
    return new SqlFixture(dbKind, createSqlContainer(dbKind));
  }

  @SuppressWarnings("resource")
  private static JdbcDatabaseContainer<?> createSqlContainer(String dbKind) {
    return switch (dbKind) {
      case "postgresql" -> new PostgreSQLContainer("postgres:16");
      case "mysql" ->
          new MySQLContainer("mysql:8.0")
              .withDatabaseName("ratchet_test")
              .withUsername("ratchet")
              .withPassword("ratchet")
              // Force the JDBC driver to treat DATETIME columns as UTC. Testcontainers' MySQL
              // defaults to server TZ = UTC, but without this URL param the driver interprets the
              // stored string as JVM-local time, shifting round-tripped Instants by the JVM's
              // offset from UTC. mysql-connector-j >=8.0.23 honors `connectionTimeZone`; older
              // pre-8.0.23 drivers use `serverTimezone`; we set both for safety.
              .withUrlParam("connectionTimeZone", "UTC")
              .withUrlParam("serverTimezone", "UTC")
              .withInitScript("ddl/mysql-schema.sql")
              .withReuse(true);
      case "oracle" ->
          new OracleContainer("gvenzl/oracle-free:slim-faststart")
              .withDatabaseName("ratchet_test")
              .withUsername("ratchet")
              .withPassword("ratchet")
              // Oracle's SGA needs far more than Docker's default 64 MB /dev/shm; without this
              // the instance OOMs while opening the database (ORA-03113 end-of-file on
              // communication channel). Round-tripped Instant correctness is handled by
              // hibernate.jdbc.time_zone=UTC rather than vendor URL params.
              .withSharedMemorySize(2L * 1024 * 1024 * 1024)
              .withStartupTimeout(Duration.ofMinutes(5))
              .withInitScript("ddl/oracle-schema.sql")
              .withReuse(true);
      case "mssql" -> {
        String image =
            System.getenv()
                .getOrDefault("RATCHET_MSSQL_IMAGE", "mcr.microsoft.com/mssql/server:2022-latest");
        DockerImageName dockerImage =
            DockerImageName.parse(image)
                .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");
        yield new MSSQLServerContainer(dockerImage)
            .acceptLicense()
            .withPassword("Ratchet!Str0ngPwd")
            .withUrlParam("trustServerCertificate", "true");
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported quarkus.datasource.db-kind for Ratchet integration tests: " + dbKind);
    };
  }

  private static void provisionRatchetDatabase(JdbcDatabaseContainer<?> c) {
    // Connect to the default database (master) to (re)create the ratchet database with RCSI.
    try (Connection master =
            DriverManager.getConnection(c.getJdbcUrl(), c.getUsername(), c.getPassword());
        Statement st = master.createStatement()) {
      st.execute(
          "IF DB_ID('ratchet') IS NOT NULL BEGIN ALTER DATABASE [ratchet] SET SINGLE_USER WITH"
              + " ROLLBACK IMMEDIATE; DROP DATABASE [ratchet]; END");
      st.execute("CREATE DATABASE [ratchet]");
      st.execute("ALTER DATABASE [ratchet] SET READ_COMMITTED_SNAPSHOT ON");
      st.execute("ALTER DATABASE [ratchet] SET ALLOW_SNAPSHOT_ISOLATION ON");
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to provision the ratchet database", e);
    }
    applySchema(c.getJdbcUrl() + ";databaseName=ratchet", c.getUsername(), c.getPassword());
  }

  private static void applySchema(String url, String user, String password) {
    // Strip -- line comments from the whole file BEFORE splitting on ';': some comment lines
    // contain a semicolon, which would otherwise split a comment and leave a fragment as SQL.
    String schema = stripComments(readSchema());
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

  /** Drops every {@code --} line comment so no comment text survives the {@code ;} split. */
  private static String stripComments(String sql) {
    return Arrays.stream(sql.split("\n"))
        .filter(line -> !line.stripLeading().startsWith("--"))
        .collect(Collectors.joining("\n"));
  }

  private static String readSchema() {
    try (InputStream in =
        RatchetDatabaseTestResource.class.getResourceAsStream("/ddl/sqlserver-schema.sql")) {
      if (in == null) {
        throw new IllegalStateException("ddl/sqlserver-schema.sql not found on the test classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Map<String, String> start() {
    return FIXTURE.config();
  }

  @Override
  public void stop() {}

  private interface DatabaseFixture {
    void start();

    Map<String, String> config();
  }

  private static final class SqlFixture implements DatabaseFixture {

    private final String dbKind;
    private final JdbcDatabaseContainer<?> container;

    private SqlFixture(String dbKind, JdbcDatabaseContainer<?> container) {
      this.dbKind = dbKind;
      this.container = container;
    }

    @Override
    public void start() {
      container.start();
      if ("mssql".equals(dbKind)) {
        provisionRatchetDatabase(container);
        container.withUrlParam("databaseName", "ratchet");
      }
    }

    @Override
    public Map<String, String> config() {
      if ("postgresql".equals(dbKind)) {
        return Map.of(
            "quarkus.datasource.jdbc.url",
            container.getJdbcUrl(),
            "quarkus.datasource.username",
            container.getUsername(),
            "quarkus.datasource.password",
            container.getPassword(),
            "ratchet.schema.auto-migrate",
            "true");
      }
      return Map.of(
          "quarkus.datasource.jdbc.url",
          container.getJdbcUrl(),
          "quarkus.datasource.username",
          container.getUsername(),
          "quarkus.datasource.password",
          container.getPassword());
    }
  }

  private static final class MongoFixture implements DatabaseFixture {

    private static final String DATABASE_NAME = "ratchet_quarkus_tck";

    private final MongoDBContainer container =
        new MongoDBContainer("mongo:7.0")
            .withReplicaSet()
            .waitingFor(
                Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Override
    public void start() {
      container.start();
    }

    @Override
    public Map<String, String> config() {
      return Map.of(
          "quarkus.mongodb.connection-string",
          container.getConnectionString(),
          "quarkus.mongodb.database",
          DATABASE_NAME,
          "quarkus.mongodb.devservices.enabled",
          "false");
    }
  }
}
