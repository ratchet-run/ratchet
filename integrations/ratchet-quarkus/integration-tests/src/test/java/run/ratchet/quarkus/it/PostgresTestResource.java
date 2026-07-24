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
import java.util.Map;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Starts PostgreSQL with standalone Testcontainers 2.0.5 instead of Quarkus Dev Services.
 *
 * <p>Quarkus 3.20 Dev Services bundles Testcontainers 1.x, whose docker-java client advertises
 * Docker API 1.32. Docker Engine 29 rejects that because its minimum API is 1.40. Standalone
 * Testcontainers 2.0.5 negotiates correctly, so this preserves zero-setup PostgreSQL provisioning
 * while avoiding the Dev Services client-version failure.
 */
public class PostgresTestResource implements QuarkusTestResourceLifecycleManager {

  private PostgreSQLContainer container;

  @Override
  public Map<String, String> start() {
    container =
        new PostgreSQLContainer("postgres:16").withInitScript("ddl/postgresql-schema.sql");
    container.start();
    return Map.of(
        "quarkus.datasource.jdbc.url",
        container.getJdbcUrl(),
        "quarkus.datasource.username",
        container.getUsername(),
        "quarkus.datasource.password",
        container.getPassword());
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }
}
