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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;

/** Exercises application transactions through the Spring checkpoint registrar and real store. */
class CheckpointTransactionTest {
  @Test
  void committedCheckpointSurvivesRetry() {
    verify(false);
  }

  @Test
  void rollbackDropsValueInSameAttemptAndRetry() {
    verify(true);
  }

  private void verify(boolean rollback) {
    try (var database = SqlDatabase.start();
        var context =
            new SpringApplicationBuilder(ConsumerApplication.class, CheckpointConfig.class)
                .properties(database.properties())
                .run()) {
      var jobs = context.getBean(CheckpointJobs.class);
      var scheduler = context.getBean(JobSchedulerService.class);
      var store = context.getBean(JobStore.class);
      UUID id =
          scheduler
              .enqueue(() -> jobs.retry(rollback))
              .withMaxRetries(1)
              .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
              .submit()
              .id();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(store.findById(id).orElseThrow().getStatus())
                      .isEqualTo(JobStatus.SUCCEEDED));
      assertThat(jobs.observed.get(id))
          .containsExactly(Optional.of("page-1"), Optional.of("page-1"));
      var row =
          store
              .capability(JobStateStore.class)
              .orElseThrow()
              .getState(id, "ratchet.checkpoint")
              .orElseThrow();
      var serializer = context.getBean(PayloadSerializer.class);
      assertThat(serializer.deserialize(row.json(), Map.class).get("value")).isEqualTo("page-1");
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CheckpointConfig {
    @Bean
    CheckpointJobs checkpointJobs(PlatformTransactionManager manager) {
      return new CheckpointJobs(manager);
    }
  }

  public static class CheckpointJobs {
    private final TransactionTemplate transaction;
    final Map<UUID, List<Optional<String>>> observed = new ConcurrentHashMap<>();

    public CheckpointJobs(PlatformTransactionManager manager) {
      transaction = new TransactionTemplate(manager);
    }

    public void retry(boolean rollback) {
      JobContext ctx = JobContext.current();
      var values = observed.computeIfAbsent(ctx.jobId(), id -> new CopyOnWriteArrayList<>());
      if (values.isEmpty()) {
        transaction.executeWithoutResult(tx -> ctx.checkpoint("page-1"));
        if (rollback) {
          transaction.executeWithoutResult(
              tx -> {
                ctx.checkpoint("page-2");
                tx.setRollbackOnly();
              });
        }
        values.add(ctx.lastCheckpoint());
        throw new IllegalStateException("Retry after committed checkpoint");
      }
      values.add(ctx.lastCheckpoint());
    }
  }
}
