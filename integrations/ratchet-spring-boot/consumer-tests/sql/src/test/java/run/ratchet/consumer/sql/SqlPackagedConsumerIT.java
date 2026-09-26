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
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.ConsumerProcess;

/** Launches the JAR or native executable without the reactor test classpath. */
class SqlPackagedConsumerIT {
  @Test
  void packagedApplicationPersistsAndExecutesARealJob() throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments =
          new ArrayList<>(
              ConsumerProcess.command(
                  "sql-consumer",
                  "--consumer.verify=true",
                  "--spring.threads.virtual.enabled=true",
                  "--consumer.full-verify=" + Boolean.getBoolean("consumer.aot"),
                  "--ratchet.encryption.enabled=" + Boolean.getBoolean("consumer.aot"),
                  "--ratchet.encryption.current-key=native-test",
                  "--ratchet.encryption.keys=native-test:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="));
      database.properties().forEach((key, value) -> arguments.add("--" + key + "=" + value));
      Path output = Path.of("target", "packaged-consumer.log");
      Process process =
          new ProcessBuilder(arguments)
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      try {
        assertThat(process.waitFor(120, TimeUnit.SECONDS))
            .withFailMessage("Consumer did not exit: %s", Files.readString(output))
            .isTrue();
        assertThat(process.exitValue()).withFailMessage("%s", Files.readString(output)).isZero();
        assertThat(Files.readString(output)).contains("RATCHET_CONSUMER_VERIFIED");
        if (Boolean.getBoolean("consumer.aot")) {
          assertThat(Files.readString(output))
              .contains(
                  "RATCHET_NATIVE_FEATURES_VERIFIED",
                  "RATCHET_NATIVE_DURABILITY_VERIFIED",
                  "RATCHET_SQL_NATIVE_VERIFIED",
                  "RATCHET_NATIVE_ADVICE_VERIFIED",
                  "RATCHET_GRACEFUL_SHUTDOWN_VERIFIED");
          assertCiphertext(database, "business_key = 'native-encrypted'", "native-secret-argument");
        }
      } finally {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  static void assertCiphertext(SqlDatabase database, String predicate, String secret)
      throws Exception {
    var properties = database.properties();
    try (var connection =
            DriverManager.getConnection(
                (String) properties.get("spring.datasource.url"),
                (String) properties.get("spring.datasource.username"),
                (String) properties.get("spring.datasource.password"));
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "select encrypted_payload, encryption_key_id, payload from scheduler_job where "
                    + predicate)) {
      assertThat(rows.next()).isTrue();
      assertThat(rows.getBoolean(1)).isTrue();
      assertThat(rows.getString(2)).isEqualTo("native-test");
      assertThat(rows.getString(3)).contains("rcph:e:").doesNotContain(secret);
      assertThat(rows.next()).isFalse();
    }
  }
}
