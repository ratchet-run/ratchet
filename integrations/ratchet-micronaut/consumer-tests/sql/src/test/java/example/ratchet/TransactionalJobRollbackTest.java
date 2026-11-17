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
package example.ratchet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.spi.JobStore;

/** Exercises rollback-only job bodies through the consumer's Hibernate manager. */
class TransactionalJobRollbackTest {
  @Test
  void nestedFailureCaughtByTheBodyFailsTheAttempt() {
    verifyRollback("nested");
  }

  @Test
  void rollbackOnlyBodyFailsTheAttempt() {
    verifyRollback("rollbackOnly");
  }

  private void verifyRollback(String scenario) {
    try (var database = SqlDatabase.start();
        var context =
            SqlApplication.start(
                database,
                "consumer.transactional-jobs=true",
                "ratchet.jpa.datasource=default",
                "ratchet.circuit-breaker.enabled=false")) {
      var jobs = context.getBean(RollbackOnlyJobs.class);
      var store = context.getBean(JobStore.class);
      String id = UUID.randomUUID().toString();
      UUID jobId =
          context
              .getBean(JobSchedulerService.class)
              .enqueue(() -> jobs.execute(id, scenario))
              .withMaxRetries(0)
              .submit()
              .id();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                var job = store.findById(jobId).orElseThrow();
                assertThat(job.getStatus())
                    .as("lastError=%s", job.getLastError())
                    .isEqualTo(JobStatus.FAILED);
              });
      assertThat(store.findById(jobId).orElseThrow().getLastError())
          .contains("marked for rollback");
      assertThat(count(database, id)).isZero();
    }
  }

  private static int count(SqlDatabase database, String id) {
    try (var connection = SqlApplication.connection(database);
        var statement =
            connection.prepareStatement("SELECT COUNT(*) FROM consumer_record WHERE id = ?")) {
      statement.setString(1, id);
      try (var rows = statement.executeQuery()) {
        if (!rows.next()) throw new IllegalStateException("Count returned no row");
        return rows.getInt(1);
      }
    } catch (SQLException failure) {
      throw new IllegalStateException("Application row probe failed", failure);
    }
  }
}
