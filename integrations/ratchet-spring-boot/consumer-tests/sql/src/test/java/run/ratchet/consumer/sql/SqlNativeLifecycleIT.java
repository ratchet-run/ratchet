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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.consumer.ConsumerProcess;

class SqlNativeLifecycleIT {
  @ParameterizedTest
  @ValueSource(strings = {"startup-failure", "drain", "unexposed"})
  void lifecycleScenarioRunsInApplicationProcess(String scenario) throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments =
          new ArrayList<>(
              ConsumerProcess.command(
                  "sql-consumer", "--consumer.verify=true", "--consumer.scenario=" + scenario));
      database.properties().forEach((key, value) -> arguments.add("--" + key + "=" + value));
      Path output = Path.of("target", "lifecycle-" + scenario + ".log");
      Process process =
          new ProcessBuilder(arguments)
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      try {
        assertThat(process.waitFor(150, TimeUnit.SECONDS))
            .withFailMessage("%s", Files.readString(output))
            .isTrue();
        assertThat(process.exitValue()).withFailMessage("%s", Files.readString(output)).isZero();
        assertThat(Files.readString(output))
            .contains(
                scenario.equals("drain")
                    ? "RATCHET_RUNNING_SHUTDOWN_VERIFIED"
                    : "RATCHET_STARTUP_RECOVERY_VERIFIED " + scenario);
      } finally {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
      if (scenario.equals("drain")) {
        var properties = database.properties();
        try (var connection =
                DriverManager.getConnection(
                    (String) properties.get("spring.datasource.url"),
                    (String) properties.get("spring.datasource.username"),
                    (String) properties.get("spring.datasource.password"));
            var statement = connection.createStatement();
            var row =
                statement.executeQuery(
                    "select terminal_status from scheduler_job where business_key = 'native-drain'")) {
          assertThat(row.next()).isTrue();
          assertThat(row.getString(1)).isEqualTo("SUCCEEDED");
          assertThat(row.next()).isFalse();
        }
      }
    }
  }
}
