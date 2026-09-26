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
package example.runtime.sql;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import run.ratchet.consumer.RuntimeProcess;
import run.ratchet.consumer.sql.SqlDatabase;

class EncryptionRotationRuntimeIT {
  static final String OLD = Base64.getEncoder().encodeToString(new byte[32]);
  static final String NEW =
      Base64.getEncoder()
          .encodeToString("12345678901234567890123456789012".getBytes(StandardCharsets.US_ASCII));

  @Test
  void rotatingWriteKeyStillExecutesOldPayloadAndEncryptsNewSubmissions() throws Exception {
    try (var database = SqlDatabase.start()) {
      var jdbc = new JdbcTemplate(RuntimeSupport.dataSource(database));
      UUID old = queueOldJob(database, "rotation-old");
      try (var rotated =
          new RuntimeProcess(
                  RuntimeWebApplication.class,
                  keys(database, "old:" + OLD + ",new:" + NEW, "new"),
                  "rotation-new")
              .ready()) {
        succeeded(jdbc, old);
        UUID fresh = UUID.fromString(rotated.post("/submit?id=rotation-new"));
        succeeded(jdbc, fresh);
        assertThat(
                jdbc.queryForObject(
                    "select encryption_key_id from scheduler_job where job_id = ?",
                    String.class,
                    fresh))
            .isEqualTo("new");
        assertThat(
                jdbc.queryForObject(
                    "select cast(payload as text) from scheduler_job where job_id = ?",
                    String.class,
                    fresh))
            .contains("rcph:e:")
            .doesNotContain("rotation-new");
        assertThat(
                jdbc.queryForObject(
                    "select executions from runtime_effect where id = 'rotation-old'",
                    Integer.class))
            .isEqualTo(1);
      }
    }
  }

  @Test
  void missingKeyFailsWithoutInvokingPayloadAndRestoredKeyAllowsExplicitReplay() throws Exception {
    try (var database = SqlDatabase.start()) {
      var jdbc = new JdbcTemplate(RuntimeSupport.dataSource(database));
      UUID original = queueOldJob(database, "missing-key");
      try (var missing =
          new RuntimeProcess(
                  RuntimeWebApplication.class, keys(database, "new:" + NEW, "new"), "missing-key")
              .ready()) {
        await()
            .atMost(Duration.ofSeconds(40))
            .untilAsserted(
                () ->
                    assertThat(
                            jdbc.queryForObject(
                                "select terminal_status from scheduler_job where job_id = ?",
                                String.class,
                                original))
                        .isEqualTo("FAILED"));
        assertThat(
                jdbc.queryForObject(
                    "select count(*) from runtime_attempt where business_id = 'missing-key'",
                    Integer.class))
            .isZero();
        assertThat(
                jdbc.queryForObject(
                    "select executions from runtime_effect where id = 'missing-key'",
                    Integer.class))
            .isZero();
        assertThat(Files.readString(missing.log)).contains("No key installed for id: old");
      }
      try (var restored =
          new RuntimeProcess(
                  RuntimeWebApplication.class,
                  keys(database, "old:" + OLD + ",new:" + NEW, "new"),
                  "restored-key")
              .ready()) {
        assertThat(restored.post("/retry?id=" + original)).isEqualTo("true");
        succeeded(jdbc, original);
        assertThat(
                jdbc.queryForObject(
                    "select executions from runtime_effect where id = 'missing-key'",
                    Integer.class))
            .isEqualTo(1);
      }
    }
  }

  static UUID queueOldJob(SqlDatabase database, String id) throws Exception {
    UUID original;
    try (var app =
        new RuntimeProcess(
                RuntimeWebApplication.class, keys(database, "old:" + OLD, "old"), id + "-original")
            .ready()) {
      app.post("/drain?enabled=true");
      original = UUID.fromString(app.post("/submit?id=" + id));
      var jdbc = new JdbcTemplate(RuntimeSupport.dataSource(database));
      assertThat(
              jdbc.queryForObject(
                  "select encryption_key_id from scheduler_job where job_id = ?",
                  String.class,
                  original))
          .isEqualTo("old");
      assertThat(
              jdbc.queryForObject(
                  "select cast(payload as text) from scheduler_job where job_id = ?",
                  String.class,
                  original))
          .contains("rcph:e:")
          .doesNotContain(id);
      assertThat(
              jdbc.queryForObject(
                  "select count(*) from runtime_attempt where business_id = ?", Integer.class, id))
          .isZero();
    }
    return original;
  }

  static Map<String, Object> keys(SqlDatabase database, String material, String current) {
    var properties = WebAndClusterRuntimeIT.properties(database, "key-worker");
    properties.put("fixture.recurring", false);
    properties.put("ratchet.encryption.enabled", true);
    properties.put("ratchet.encryption.keys", material);
    properties.put("ratchet.encryption.current-key", current);
    return properties;
  }

  static void succeeded(JdbcTemplate jdbc, UUID id) {
    await()
        .atMost(Duration.ofSeconds(40))
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForObject(
                            "select terminal_status from scheduler_job where job_id = ?",
                            String.class,
                            id))
                    .isEqualTo("SUCCEEDED"));
  }
}
