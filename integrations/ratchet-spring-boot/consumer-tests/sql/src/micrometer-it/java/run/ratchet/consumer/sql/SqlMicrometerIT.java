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
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.ConsumerProcess;

class SqlMicrometerIT {
  @Test
  void queueGaugesFollowSchedulerLifecycle() throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments =
          new ArrayList<>(
              ConsumerProcess.command(
                  "sql-consumer", "--consumer.verify=true", "--consumer.scenario=micrometer"));
      database.properties().forEach((key, value) -> arguments.add("--" + key + "=" + value));
      Path output = Path.of("target", "micrometer.log");
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
        assertThat(Files.readString(output)).contains("RATCHET_MICROMETER_VERIFIED");
      } finally {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }
}
