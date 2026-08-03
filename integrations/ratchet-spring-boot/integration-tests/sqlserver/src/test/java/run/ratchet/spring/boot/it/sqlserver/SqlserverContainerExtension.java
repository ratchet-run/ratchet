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
package run.ratchet.spring.boot.it.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

/** Starts one SQL Server container per test JVM and explicitly stops it at root-context close. */
public final class SqlserverContainerExtension implements BeforeAllCallback {

  private static final String DEFAULT_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
  private static final String DB_NAME = "ratchet";
  private static final String PASSWORD = "Ratchet!Str0ngPwd";

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(SqlserverContainerExtension.class);
  private static final String RESOURCE_KEY = "sqlserver";

  private static volatile ContainerResource resource;

  @Override
  public void beforeAll(ExtensionContext context) {
    resource =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                RESOURCE_KEY, ignored -> currentOrCreateResource(), ContainerResource.class);
  }

  public static String jdbcUrl() {
    return container().getJdbcUrl();
  }

  public static String username() {
    return container().getUsername();
  }

  public static String password() {
    return container().getPassword();
  }

  private static MSSQLServerContainer container() {
    return currentOrCreateResource().container;
  }

  private static ContainerResource currentOrCreateResource() {
    ContainerResource current = resource;
    if (current != null) {
      return current;
    }
    synchronized (SqlserverContainerExtension.class) {
      current = resource;
      if (current == null) {
        current = new ContainerResource();
        resource = current;
      }
      return current;
    }
  }

  private static final class ContainerResource implements ExtensionContext.Store.CloseableResource {

    private final MSSQLServerContainer container = createContainer();

    private ContainerResource() {
      container.start();
      provisionRatchetDatabase(container);
      // All later JDBC URLs target the RCSI-enabled database rather than master.
      container.withUrlParam("databaseName", DB_NAME);
    }

    @Override
    public void close() {
      container.stop();
    }
  }

  private static MSSQLServerContainer createContainer() {
    String image = System.getenv().getOrDefault("RATCHET_MSSQL_IMAGE", DEFAULT_IMAGE);
    DockerImageName dockerImage =
        DockerImageName.parse(image).asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");
    return new MSSQLServerContainer(dockerImage)
        .acceptLicense()
        .withPassword(PASSWORD)
        .withUrlParam("trustServerCertificate", "true");
  }

  private static void provisionRatchetDatabase(MSSQLServerContainer container) {
    try (Connection master =
            DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
        Statement statement = master.createStatement()) {
      statement.execute(
          "IF DB_ID('"
              + DB_NAME
              + "') IS NOT NULL BEGIN ALTER DATABASE ["
              + DB_NAME
              + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["
              + DB_NAME
              + "]; END");
      statement.execute("CREATE DATABASE [" + DB_NAME + "]");
      statement.execute("ALTER DATABASE [" + DB_NAME + "] SET READ_COMMITTED_SNAPSHOT ON");
      statement.execute("ALTER DATABASE [" + DB_NAME + "] SET ALLOW_SNAPSHOT_ISOLATION ON");
    } catch (SQLException exception) {
      throw new IllegalStateException("Failed to provision the ratchet database", exception);
    }
  }
}
