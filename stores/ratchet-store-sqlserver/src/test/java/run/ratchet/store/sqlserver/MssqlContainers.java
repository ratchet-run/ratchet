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
package run.ratchet.store.sqlserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Factory for the SQL Server Testcontainer shared by the store's TCK fixtures.
 *
 * <p>Defaults to {@code mcr.microsoft.com/mssql/server:2022-latest} (matches the CI/amd64 target;
 * runs under emulation on Apple Silicon). This is the reference image — the claim path's
 * boosted-priority ordering uses {@code GREATEST}, which exists only in SQL Server 2022+. The
 * {@code RATCHET_MSSQL_IMAGE} environment variable can substitute another SQL-Server-compatible
 * image (accepted as a substitute for the canonical one), but the arm64-native {@code
 * azure-sql-edge} is a SQL Server 2019 engine and will not pass the claim contracts.
 *
 * <p>{@code trustServerCertificate=true} is required because mssql-jdbc 12+ defaults to an
 * encrypted connection and the container presents a self-signed certificate.
 *
 * <p>The shared instance runs against a dedicated {@code ratchet} database with {@code
 * READ_COMMITTED_SNAPSHOT} enabled. SQL Server's default lock-based READ COMMITTED takes shared
 * read locks (unlike the MVCC PostgreSQL/MySQL engines Ratchet targets), so concurrent claim/cancel
 * paths deadlock; row-versioning snapshot reads restore the non-blocking semantics the store
 * assumes. RCSI cannot be set on {@code master}, hence the dedicated database.
 */
final class MssqlContainers {

  private static final String DEFAULT_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
  private static final String DB_NAME = "ratchet";
  static final String PASSWORD = "Ratchet!Str0ngPwd";

  private MssqlContainers() {}

  static MSSQLServerContainer create() {
    String image = System.getenv().getOrDefault("RATCHET_MSSQL_IMAGE", DEFAULT_IMAGE);
    DockerImageName dockerImage =
        DockerImageName.parse(image).asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");
    return new MSSQLServerContainer(dockerImage)
        .acceptLicense()
        .withPassword(PASSWORD)
        .withUrlParam("trustServerCertificate", "true");
  }

  /** The single schema-loaded SQL Server container shared by every surefire test class. */
  private static final class Shared {
    private static final MSSQLServerContainer INSTANCE = create().withReuse(true);

    static {
      INSTANCE.start();
      provisionRatchetDatabase(INSTANCE);
      // Subsequent getJdbcUrl() calls (used by the JPA fixture and the conformance test) now target
      // the RCSI-enabled ratchet database instead of master.
      INSTANCE.withUrlParam("databaseName", DB_NAME);
    }
  }

  static MSSQLServerContainer shared() {
    return Shared.INSTANCE;
  }

  private static void provisionRatchetDatabase(MSSQLServerContainer c) {
    // Connect to the default database (master) to (re)create the ratchet database with RCSI.
    try (Connection master =
            DriverManager.getConnection(c.getJdbcUrl(), c.getUsername(), c.getPassword());
        Statement st = master.createStatement()) {
      st.execute(
          "IF DB_ID('"
              + DB_NAME
              + "') IS NOT NULL BEGIN ALTER DATABASE ["
              + DB_NAME
              + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["
              + DB_NAME
              + "]; END");
      st.execute("CREATE DATABASE [" + DB_NAME + "]");
      st.execute("ALTER DATABASE [" + DB_NAME + "] SET READ_COMMITTED_SNAPSHOT ON");
      st.execute("ALTER DATABASE [" + DB_NAME + "] SET ALLOW_SNAPSHOT_ISOLATION ON");
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to provision the ratchet database", e);
    }
    applySchema(c.getJdbcUrl() + ";databaseName=" + DB_NAME, c.getUsername(), c.getPassword());
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
    try (InputStream in = MssqlContainers.class.getResourceAsStream("/ddl/sqlserver-schema.sql")) {
      if (in == null) {
        throw new IllegalStateException("ddl/sqlserver-schema.sql not found on the test classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
