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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootVersion;

/** Executes the packaged consumer using only Boot's connection properties and shipped resources. */
class MongoPackagedConsumerIT {
  @Test
  void executableJarPersistsAndExecutesARealJob() throws Exception {
    try (var database = MongoDatabase.create()) {
      database.start();
      String uriProperty =
          SpringBootVersion.getVersion().startsWith("3.")
              ? "spring.data.mongodb.uri"
              : "spring.mongodb.uri";
      Path output = Path.of("target", "packaged-consumer.log");
      Process process =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-jar",
                  "target/mongodb-consumer-1.0-SNAPSHOT.jar",
                  "--consumer.verify=true",
                  "--" + uriProperty + "=" + database.getReplicaSetUrl("ratchet_packaged"))
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      try {
        assertThat(process.waitFor(120, TimeUnit.SECONDS))
            .withFailMessage("Consumer did not exit: %s", Files.readString(output))
            .isTrue();
        assertThat(process.exitValue()).withFailMessage("%s", Files.readString(output)).isZero();
        assertThat(Files.readString(output)).contains("RATCHET_MONGODB_CONSUMER_VERIFIED");
      } finally {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }
}
