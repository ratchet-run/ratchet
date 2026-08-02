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
package run.ratchet.spring.boot.it.nativepostgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** JVM control proving the native scenarios against the same real PostgreSQL-backed Boot app. */
@ExtendWith(NativePostgresqlJvmControlTest.PostgresqlExtension.class)
class NativePostgresqlJvmControlTest {

  @Test
  void allNativePostgresqlScenariosPassOnTheJvm() {
    try (ConfigurableApplicationContext context =
        new SpringApplicationBuilder(NativePostgresqlApplication.class)
            .web(WebApplicationType.NONE)
            .registerShutdownHook(false)
            .run(
                "--native-postgresql.run-on-startup=false",
                "--spring.datasource.url=" + PostgresqlExtension.container.getJdbcUrl(),
                "--spring.datasource.username=" + PostgresqlExtension.container.getUsername(),
                "--spring.datasource.password=" + PostgresqlExtension.container.getPassword())) {
      List<NativePostgresqlScenarios.Evidence> evidence =
          context.getBean(NativePostgresqlScenarios.class).runAll();

      assertEquals(
          NativePostgresqlScenarios.REQUIRED_SCENARIOS,
          evidence.stream().map(NativePostgresqlScenarios.Evidence::scenario).toList());
      assertTrue(
          evidence.stream().allMatch(NativePostgresqlScenarios.Evidence::passed),
          () ->
              evidence.stream()
                  .filter(result -> !result.passed())
                  .map(result -> result.scenario() + ": " + result.detail())
                  .toList()
                  .toString());
    }
  }

  static final class PostgresqlExtension implements BeforeAllCallback, AfterAllCallback {

    private static final PostgreSQLContainer container =
        new PostgreSQLContainer("postgres:16")
            .withDatabaseName("ratchet_spring_boot_native")
            .withUsername("ratchet")
            .withPassword("ratchet");

    @Override
    public void beforeAll(ExtensionContext context) {
      container.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
      container.stop();
    }
  }
}
