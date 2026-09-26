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
package run.ratchet.consumer.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringBootVersion;
import run.ratchet.consumer.ConsumerProcess;

class MongoNativeLifecycleIT {
  @ParameterizedTest
  @ValueSource(strings = {"startup-failure", "drain"})
  void lifecycleScenarioRunsInApplicationProcess(String scenario) throws Exception {
    try (var database = MongoDatabase.create()) {
      database.start();
      String uri = database.getReplicaSetUrl("ratchet_lifecycle");
      String property =
          SpringBootVersion.getVersion().startsWith("3.")
              ? "spring.data.mongodb.uri"
              : "spring.mongodb.uri";
      var arguments =
          ConsumerProcess.command(
              "mongodb-consumer",
              "--consumer.verify=true",
              "--consumer.scenario=" + scenario,
              "--" + property + "=" + uri);
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
        try (var client = MongoClients.create(uri)) {
          var row =
              client
                  .getDatabase("ratchet_lifecycle")
                  .getCollection("scheduler_job")
                  .find(Filters.eq("business_key", "native-drain"))
                  .first();
          assertThat(row).isNotNull();
          assertThat(row.getString("status")).isEqualTo("SUCCEEDED");
        }
      }
    }
  }
}
