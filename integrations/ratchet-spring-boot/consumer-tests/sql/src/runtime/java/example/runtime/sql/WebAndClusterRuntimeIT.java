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

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import run.ratchet.consumer.RuntimeProcess;
import run.ratchet.consumer.sql.SqlDatabase;

class WebAndClusterRuntimeIT {
  static Map<String, Object> properties(SqlDatabase database, String node) {
    var properties = RuntimeSupport.properties(database);
    properties.put("spring.main.web-application-type", "servlet");
    properties.put("server.port", 0);
    properties.put("server.shutdown", "graceful");
    properties.put("spring.lifecycle.timeout-per-shutdown-phase", "30s");
    properties.put("ratchet.shutdown-timeout", "25s");
    properties.put("ratchet.node.id", node);
    properties.put("ratchet.node.heartbeat-interval-seconds", 1);
    properties.put("ratchet.node.orphan-grace-seconds", 3);
    properties.put("ratchet.node.orphan-scan-interval-minutes", 1);
    properties.put("ratchet.node.dynamic-heartbeat-enabled", false);
    return properties;
  }

  @Test
  void httpTransactionsAndSigtermDrainBeforeDataSourceCloses() throws Exception {
    try (var database = SqlDatabase.start();
        var app =
            new RuntimeProcess(
                    RuntimeWebApplication.class, properties(database, "web"), "web-sigterm")
                .ready()) {
      var jdbc = new JdbcTemplate(RuntimeSupport.dataSource(database));
      assertThatThrownBy(() -> app.post("/submit?id=rollback&rollback=true"))
          .isInstanceOf(AssertionError.class)
          .hasMessageContaining("HTTP 500");
      assertThat(
              jdbc.queryForObject(
                  "select count(*) from runtime_effect where id = 'rollback'", Integer.class))
          .isZero();
      assertThat(
              jdbc.queryForObject(
                  "select count(*) from scheduler_job where business_key = 'rollback'",
                  Integer.class))
          .isZero();
      jdbc.update("insert into runtime_gate values ('shutdown', false)");
      UUID id = UUID.fromString(app.post("/submit?id=drained&gate=shutdown"));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(
                          jdbc.queryForObject(
                              "select count(*) from runtime_attempt where business_id = 'drained'",
                              Integer.class))
                      .isEqualTo(1));
      app.process.destroy(); // actual SIGTERM and Spring's JVM shutdown hook
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Files.readString(app.log).contains("RATCHET_RUNTIME_STOPPING"));
      await()
          .during(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(3))
          .until(() -> app.process.isAlive());
      jdbc.update("update runtime_gate set released = true where id = 'shutdown'");
      assertThat(app.process.waitFor(40, TimeUnit.SECONDS)).isTrue();
      assertThat(app.process.exitValue()).isEqualTo(143);
      assertThat(Files.readString(app.log)).contains("RATCHET_RUNTIME_STOPPED_DB_AVAILABLE");
      assertThat(
              jdbc.queryForObject(
                  "select terminal_status from scheduler_job where job_id = ?", String.class, id))
          .isEqualTo("SUCCEEDED");
      assertThat(
              jdbc.queryForObject(
                  "select executions from runtime_effect where id = 'drained'", Integer.class))
          .isEqualTo(1);
    }
  }

  @Test
  void liveWorkersDeduplicateLimitConcurrencyAndRecoverWithoutRestartingSurvivor()
      throws Exception {
    try (var database = SqlDatabase.start();
        var first =
            new RuntimeProcess(
                    RuntimeWebApplication.class, properties(database, "worker-one"), "cluster-one")
                .ready();
        var second =
            new RuntimeProcess(
                    RuntimeWebApplication.class, properties(database, "worker-two"), "cluster-two")
                .ready()) {
      var jdbc = new JdbcTemplate(RuntimeSupport.dataSource(database));
      assertThat(jdbc.queryForObject("select count(*) from scheduler_recurring_job", Integer.class))
          .isEqualTo(1);
      first.post("/drain?enabled=true");
      second.post("/drain?enabled=true");
      jdbc.update("insert into runtime_effect values ('raced', 0, 'submitted')");
      var a = CompletableFuture.supplyAsync(() -> submit(first, "/submit?id=raced&prepared=true"));
      var b = CompletableFuture.supplyAsync(() -> submit(second, "/submit?id=raced&prepared=true"));
      String aResult = a.get(25, TimeUnit.SECONDS);
      String bResult = b.get(25, TimeUnit.SECONDS);
      assertThat(aResult != null || bResult != null).isTrue();
      String firstId = first.post("/submit?id=raced&prepared=true");
      if (aResult != null) assertThat(aResult).isEqualTo(firstId);
      if (bResult != null) assertThat(bResult).isEqualTo(firstId);
      assertThat(
              jdbc.queryForObject(
                  "select count(*) from scheduler_job where business_key = 'raced'", Integer.class))
          .isEqualTo(1);
      jdbc.update(
          "insert into scheduler_resource_limit (resource_name, max_concurrent) values ('runtime-serial', 1)");
      var serial = new ArrayList<UUID>();
      for (int i = 0; i < 8; i++)
        serial.add(
            UUID.fromString(
                (i % 2 == 0 ? first : second)
                    .post("/submit?id=serial-" + i + "&resource=runtime-serial")));
      first.post("/drain?enabled=false");
      second.post("/drain?enabled=false");
      await()
          .atMost(Duration.ofSeconds(50))
          .untilAsserted(
              () ->
                  assertThat(
                          jdbc.queryForObject(
                              "select count(*) from scheduler_job where business_key like 'serial-%' and terminal_status = 'SUCCEEDED'",
                              Integer.class))
                      .isEqualTo(8));
      assertThat(
              jdbc.queryForObject(
                  "select count(*) from runtime_attempt a join runtime_attempt b on a.attempt_id < b.attempt_id and (b.finished_at is null or a.started_at < b.finished_at) and (a.finished_at is null or b.started_at < a.finished_at) where a.business_id like 'serial-%' and b.business_id like 'serial-%'",
                  Integer.class))
          .isZero();
      assertThat(
              jdbc.queryForObject(
                  "select executions from runtime_effect where id = 'raced'", Integer.class))
          .isEqualTo(1);
      assertThat(first.post("/submit?id=raced&prepared=true")).isEqualTo(firstId);

      second.post("/drain?enabled=true");
      jdbc.update("insert into runtime_gate values ('crash', false)");
      UUID original = UUID.fromString(first.post("/submit?id=recovered&gate=crash"));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(
                          jdbc.queryForObject(
                              "select node_id from runtime_attempt where business_id = 'recovered'",
                              String.class))
                      .isEqualTo("worker-one"));
      first.kill();
      jdbc.update("update runtime_gate set released = true where id = 'crash'");
      second.post("/drain?enabled=false");
      await()
          .atMost(Duration.ofSeconds(100))
          .untilAsserted(
              () ->
                  assertThat(
                          jdbc.queryForObject(
                              "select terminal_status from scheduler_job where job_id = ?",
                              String.class,
                              original))
                      .isEqualTo("SUCCEEDED"));
      assertThat(second.process.isAlive()).isTrue();
      assertThat(
              jdbc.queryForObject(
                  "select executions from runtime_effect where id = 'recovered'", Integer.class))
          .isEqualTo(1);
      assertThat(
              jdbc.queryForList(
                  "select distinct job_id from runtime_attempt where business_id = 'recovered'",
                  UUID.class))
          .containsExactly(original);
      assertThat(
              jdbc.queryForList(
                  "select distinct node_id from runtime_attempt where business_id = 'recovered'",
                  String.class))
          .containsExactlyInAnyOrder("worker-one", "worker-two");
    }
  }

  private static String submit(RuntimeProcess process, String path) {
    try {
      return process.post(path);
    } catch (RuntimeProcess.HttpFailure conflict) {
      assertThat(conflict.status).isEqualTo(409);
      assertThat(conflict.body).isEqualTo("duplicate-idempotency");
      return null;
    } catch (Exception failure) {
      throw new RuntimeException(failure);
    }
  }
}
