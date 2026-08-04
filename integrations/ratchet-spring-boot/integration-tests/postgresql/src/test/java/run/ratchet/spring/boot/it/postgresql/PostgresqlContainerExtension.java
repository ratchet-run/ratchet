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
package run.ratchet.spring.boot.it.postgresql;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Starts one PostgreSQL container per test JVM and explicitly stops it at root-context close. */
public final class PostgresqlContainerExtension implements BeforeAllCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(PostgresqlContainerExtension.class);
  private static final String RESOURCE_KEY = "postgresql";

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

  String currentJdbcUrl() {
    return container().getJdbcUrl();
  }

  String currentUsername() {
    return container().getUsername();
  }

  String currentPassword() {
    return container().getPassword();
  }

  private static PostgreSQLContainer container() {
    return currentOrCreateResource().container;
  }

  private static ContainerResource currentOrCreateResource() {
    ContainerResource current = resource;
    if (current != null) {
      return current;
    }
    synchronized (PostgresqlContainerExtension.class) {
      current = resource;
      if (current == null) {
        current = new ContainerResource();
        resource = current;
      }
      return current;
    }
  }

  private static final class ContainerResource implements ExtensionContext.Store.CloseableResource {

    private final PostgreSQLContainer container =
        new PostgreSQLContainer("postgres:16")
            .withDatabaseName("ratchet_spring_boot")
            .withUsername("ratchet")
            .withPassword("ratchet");

    private ContainerResource() {
      container.start();
    }

    @Override
    public void close() {
      container.stop();
    }
  }
}
