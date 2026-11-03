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
package run.ratchet.consumer.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.ConsumerProcess;
import run.ratchet.consumer.ProcessScenarios;

/** No init script or host schema provisioning: only Ratchet startup creates the schema. */
class SqlNativeMigrationIT {
  @Test
  void migratesEmptyDatabase() throws Exception {
    migrate(Map.of(), "target/native-migration.log");
  }

  /** The pom copies the store's scripts to ddl/migrations/consumer-app in the application. */
  @Test
  void migratesFromApplicationFolder() throws Exception {
    migrate(
        Map.of("ratchet.schema.migration-prefix", "ddl/migrations/consumer-app"),
        "target/native-migration-application-folder.log");
  }

  private static void migrate(Map<String, Object> overrides, String log) throws Exception {
    try (var database = SqlDatabase.start()) {
      var options = ProcessScenarios.options(database.properties());
      options.put("micronaut.environments", database.store());
      options.put("jpa.default.properties.hibernate.hbm2ddl.auto", "none");
      options.put("consumer.scenario", "migration");
      options.put("ratchet.schema.auto-migrate", true);
      options.putAll(overrides);
      try (var process = ConsumerProcess.start("sql-consumer", options, Path.of(log))) {
        assertThat(process.awaitExit()).withFailMessage("%s", process.output()).isZero();
        assertThat(process.output()).contains("RATCHET_CHECK DURABLE");
      }
      try (var connection = SqlApplication.connection(database);
          var statement = connection.createStatement()) {
        try (var rows = statement.executeQuery("select count(*) from ratchet_schema_version")) {
          assertThat(rows.next()).isTrue();
          assertThat(rows.getInt(1)).isPositive();
        }
        try (var rows =
            statement.executeQuery(
                "select terminal_status from scheduler_job where terminal_status = 'SUCCEEDED'")) {
          assertThat(rows.next()).isTrue();
        }
      }
    }
  }
}
