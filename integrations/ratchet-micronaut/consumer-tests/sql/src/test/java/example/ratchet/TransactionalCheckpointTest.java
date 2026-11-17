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

import io.micronaut.context.ApplicationContext;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;

/** Exercises checkpoint units through a real SQL consumer and its named transaction managers. */
class TransactionalCheckpointTest {
  @Test
  void unitsCommitApplicationRowAndCheckpointTogether() throws Exception {
    try (var database = SqlDatabase.start();
        var context = application(database, false)) {
      var jobs = context.getBean(CheckpointUnitJobs.class);
      String prefix = UUID.randomUUID().toString();
      UUID id = submit(context, jobs, prefix, "atomic");
      try {
        assertThat(jobs.inside.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(count(database, prefix + "-0")).isZero();
        assertThat(states(context).getState(id, "ratchet.checkpoint")).isEmpty();
      } finally {
        jobs.release.countDown();
      }
      awaitSucceeded(context, id);
      assertThat(count(database, prefix + "-0")).isEqualTo(1);
      assertCheckpoint(context, id, "done");
    }
  }

  @Test
  void retryAfterFailedUnitResumesAfterLastCommittedUnit() {
    try (var database = SqlDatabase.start();
        var context = application(database, false)) {
      var jobs = context.getBean(CheckpointUnitJobs.class);
      String prefix = UUID.randomUUID().toString();
      UUID id = submit(context, jobs, prefix, "retry");
      awaitSucceeded(context, id);
      assertThat(jobs.retryFirst).isEqualTo("1");
      assertThat(jobs.received)
          .containsExactly(Optional.empty(), Optional.of("1"), Optional.of("1"), Optional.of("2"));
      for (int page = 0; page < 3; page++)
        assertThat(count(database, prefix + "-" + page)).isEqualTo(1);
      assertCheckpoint(context, id, "done");
    }
  }

  @Test
  void transactionOnSecondDatasourceRejectsUnitBeforeItRuns() {
    verifyRejected("other", true);
  }

  @Test
  void rollbackOnlyUnitThrowsAndSavesNothing() {
    verifyRejected("rollbackOnly", false);
  }

  private void verifyRejected(String scenario, boolean secondDatasource) {
    try (var database = SqlDatabase.start();
        var context = application(database, secondDatasource)) {
      var jobs = context.getBean(CheckpointUnitJobs.class);
      String prefix = UUID.randomUUID().toString();
      UUID id = submit(context, jobs, prefix, scenario);
      awaitSucceeded(context, id);
      assertThat(jobs.caught).isInstanceOf(IllegalStateException.class);
      if (secondDatasource) {
        assertThat(jobs.otherTransactionActive).isTrue();
        assertThat(jobs.unitRan).isFalse();
      }
      assertThat(count(database, prefix + "-0")).isZero();
      assertThat(states(context).getState(id, "ratchet.checkpoint")).isEmpty();
    }
  }

  private static ApplicationContext application(SqlDatabase database, boolean secondDatasource) {
    List<String> overrides =
        new ArrayList<>(
            List.of(
                "consumer.checkpoint-units=true",
                "ratchet.jpa.datasource=default",
                "ratchet.circuit-breaker.enabled=false"));
    if (secondDatasource) {
      var properties = database.properties();
      for (String name : List.of("url", "driver-class-name", "username", "password"))
        overrides.add(
            "datasources.other." + name + "=" + properties.get("datasources.default." + name));
      overrides.add("datasources.other.transaction-manager=jdbc");
    }
    return SqlApplication.start(database, overrides.toArray(String[]::new));
  }

  private static UUID submit(
      ApplicationContext context, CheckpointUnitJobs jobs, String prefix, String scenario) {
    return context
        .getBean(JobSchedulerService.class)
        .enqueue(() -> jobs.execute(prefix, scenario))
        .withMaxRetries(scenario.equals("retry") ? 1 : 0)
        .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
        .submit()
        .id();
  }

  private static void awaitSucceeded(ApplicationContext context, UUID id) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(context.getBean(JobStore.class).findById(id).orElseThrow().getStatus())
                    .isEqualTo(JobStatus.SUCCEEDED));
  }

  private static JobStateStore states(ApplicationContext context) {
    return context.getBean(JobStore.class).capability(JobStateStore.class).orElseThrow();
  }

  private static void assertCheckpoint(ApplicationContext context, UUID id, String expected) {
    var state = states(context).getState(id, "ratchet.checkpoint").orElseThrow();
    assertThat(
            context
                .getBean(PayloadSerializer.class)
                .deserialize(state.json(), Map.class)
                .get("value"))
        .isEqualTo(expected);
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
