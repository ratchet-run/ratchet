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
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Proves a killed packaged worker is recovered by a fresh process with the same node identity. */
class SqlRecoveryIT {
  private static final String NODE_ID = "packaged-recovery-node";

  @Test
  void killedWorkerIsRecoveredAndItsOriginalJobSucceedsAfterRestart() throws Exception {
    try (SqlDatabase database = SqlDatabase.start()) {
      String id = "recovery-" + System.nanoTime();
      Path firstLog = Path.of("target", "recovery-first.log");
      Process first = start(database, firstLog, id, true);
      String originalIdentity;
      String identityQuery =
          "SELECT "
              + switch (database.store()) {
                case "postgresql" -> "CAST(job_id AS varchar)";
                case "mysql" -> "BIN_TO_UUID(job_id)";
                case "oracle" -> "RAWTOHEX(job_id)";
                case "sqlserver" -> "CONVERT(varchar(32), job_id, 2)";
                default -> throw new IllegalStateException(database.store());
              }
              + " FROM scheduler_job";
      try {
        await()
            .ignoreExceptionsInstanceOf(SQLException.class)
            .atMost(Duration.ofSeconds(60))
            .untilAsserted(
                () -> {
                  assertThat(first.isAlive())
                      .withFailMessage(
                          "First consumer exited before claiming work: %s",
                          Files.readString(firstLog))
                      .isTrue();
                  assertThat(query(database, "SELECT state FROM consumer_record"))
                      .isEqualTo("submitted");
                  assertThat(query(database, "SELECT status FROM scheduler_job_queue"))
                      .isEqualTo("RUNNING");
                });
        originalIdentity = query(database, identityQuery);
        SqlPackagedConsumerIT.assertCiphertext(database, "1 = 1", id);
      } finally {
        first.destroyForcibly();
        assertThat(first.waitFor(20, TimeUnit.SECONDS)).isTrue();
      }

      Path secondLog = Path.of("target", "recovery-second.log");
      Process second = start(database, secondLog, id, false);
      try {
        assertThat(second.waitFor(120, TimeUnit.SECONDS))
            .withFailMessage("Recovered consumer did not exit: %s", Files.readString(secondLog))
            .isTrue();
        assertThat(second.exitValue()).withFailMessage("%s", Files.readString(secondLog)).isZero();
        assertThat(Files.readString(secondLog)).contains("RATCHET_CONSUMER_VERIFIED");
      } finally {
        second.destroyForcibly();
        second.waitFor(10, TimeUnit.SECONDS);
      }

      assertThat(query(database, identityQuery)).isEqualTo(originalIdentity);
      assertThat(query(database, "SELECT COUNT(*) FROM scheduler_job")).isEqualTo("1");
      assertThat(query(database, "SELECT state FROM consumer_record")).isEqualTo("executed");
      assertThat(query(database, "SELECT terminal_status FROM scheduler_job"))
          .isEqualTo("SUCCEEDED");
      SqlPackagedConsumerIT.assertCiphertext(database, "1 = 1", id);
    }
  }

  private static Process start(SqlDatabase database, Path log, String id, boolean block)
      throws Exception {
    List<String> arguments =
        new ArrayList<>(
            run.ratchet.consumer.ConsumerProcess.command(
                "sql-consumer",
                "--consumer.verify=true",
                "--ratchet.encryption.enabled=true",
                "--ratchet.encryption.current-key=native-test",
                "--ratchet.encryption.keys=native-test:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "--consumer.verify-id=" + id,
                "--consumer.verify-timeout-seconds=60",
                "--ratchet.node.id=" + NODE_ID));
    if (!block) {
      arguments.add("--consumer.verify-submit=false");
    } else {
      arguments.add("--consumer.block-job-id=" + id);
    }
    database.properties().forEach((key, value) -> arguments.add("--" + key + "=" + value));
    return new ProcessBuilder(arguments)
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .start();
  }

  private static String query(SqlDatabase database, String sql) throws Exception {
    Map<String, Object> properties = database.properties();
    try (var connection =
            DriverManager.getConnection(
                (String) properties.get("spring.datasource.url"),
                (String) properties.get("spring.datasource.username"),
                (String) properties.get("spring.datasource.password"));
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertThat(result.next()).isTrue();
      return result.getString(1);
    }
  }
}
