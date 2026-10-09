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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import example.ratchet.ConsumerService;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;
import run.ratchet.store.spi.JobStore;

class SqlLifecycleTest {
  @Test
  void sequentialContextsReleaseOwnershipAndExecuteAgain() {
    try (var database = SqlDatabase.start()) {
      for (int i = 0; i < 2; i++) {
        String id = "sequential-" + i;
        try (var context = SqlApplication.start(database)) {
          context.getBean(ConsumerService.class).submit(id);
          var jdbc = new SqlAccess(context);
          await()
              .atMost(Duration.ofSeconds(30))
              .untilAsserted(
                  () ->
                      assertThat(
                              jdbc.queryForObject(
                                  "select state from consumer_record where id = ?",
                                  String.class,
                                  id))
                          .isEqualTo("executed"));
        }
      }
    }
  }

  @Test
  void fatalStartupFailureReleasesRuntimeOwnership() {
    try (var database = SqlDatabase.start()) {
      assertThatThrownBy(() -> SqlApplication.start(database, "consumer.fail-start=true"))
          .hasStackTraceContaining("intentional lifecycle startup failure");
      try (var recovered = SqlApplication.start(database)) {
        recovered.getBean(ConsumerService.class).submit("recovered-startup");
        var jdbc = new SqlAccess(recovered);
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () ->
                    assertThat(
                            jdbc.queryForObject(
                                "select state from consumer_record where id = 'recovered-startup'",
                                String.class))
                        .isEqualTo("executed"));
      }
    }
  }

  @Test
  void disabledIntegrationHasNoSchedulerStoreOrRatchetSchema() throws Exception {
    try (var database = SqlDatabase.start();
        var context = SqlApplication.start(database, "ratchet.enabled=false")) {
      assertThat(context.containsBean(JobSchedulerService.class)).isFalse();
      assertThat(context.containsBean(JobStore.class)).isFalse();
      try (var connection = SqlApplication.connection(database);
          var tables =
              connection
                  .getMetaData()
                  .getTables(connection.getCatalog(), connection.getSchema(), "%", null)) {
        while (tables.next()) {
          assertThat(tables.getString("TABLE_NAME").toLowerCase())
              .doesNotStartWith("scheduler_")
              .isNotEqualTo("ratchet_schema_version");
        }
      }
      // Host JPA continues to work without Ratchet.
      new SqlAccess(context)
          .update("insert into consumer_record (id, state) values ('disabled-host', 'available')");
      assertThat(
              new SqlAccess(context)
                  .queryForObject(
                      "select state from consumer_record where id = 'disabled-host'", String.class))
          .isEqualTo("available");
    }
  }

  @Test
  void shutdownWaitsForRunningWorkAndPersistsSuccessBeforeDatasourceCloses() throws Exception {
    BlockingJob.entered = new CountDownLatch(1);
    BlockingJob.release = new CountDownLatch(1);
    BlockingJob.interrupted.set(false);
    StopProbe.entered = new CountDownLatch(1);
    try (var database = SqlDatabase.start()) {
      var context =
          SqlApplication.start(database, "consumer.blocking=true", "ratchet.shutdown-timeout=20s");
      var closingThread = Executors.newSingleThreadExecutor();
      try {
        var jobs = context.getBean(BlockingJob.class);
        var handle = context.getBean(JobSchedulerService.class).enqueue(jobs::run).submit();
        assertThat(BlockingJob.entered.await(30, TimeUnit.SECONDS)).isTrue();
        var closed = closingThread.submit(context::close);
        assertThat(StopProbe.entered.await(5, TimeUnit.SECONDS)).isTrue();
        // Bounded observation proves close is waiting, rather than merely queued on the executor.
        await()
            .during(Duration.ofMillis(300))
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(closed.isDone()).isFalse());
        BlockingJob.release.countDown();
        closed.get(30, TimeUnit.SECONDS);
        assertThat(BlockingJob.interrupted.get()).isFalse();
        try (var connection = SqlApplication.connection(database);
            var statement =
                connection.prepareStatement(
                    "select terminal_status from scheduler_job where "
                        + database.jobIdPredicate())) {
          statement.setObject(1, database.queryId(handle.id()));
          try (var rows = statement.executeQuery()) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("SUCCEEDED");
          }
        }
        try (var connection = SqlApplication.connection(database);
            var statement = connection.createStatement();
            var rows =
                statement.executeQuery("select state from consumer_record where id = 'drained'")) {
          assertThat(rows.next()).isTrue();
          assertThat(rows.getString(1)).isEqualTo("executed");
        }
      } finally {
        BlockingJob.release.countDown();
        context.close();
        closingThread.shutdownNow();
        assertThat(closingThread.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  @Singleton
  @Requires(property = "consumer.blocking", value = "true")
  public static class StopProbe implements SchedulerLifecycleHook {
    static CountDownLatch entered;

    public void beforeStop() {
      entered.countDown();
    }
  }

  @Singleton
  @Requires(property = "consumer.fail-start", value = "true")
  public static class FailingHook implements SchedulerLifecycleHook {
    public void beforeStart() {
      throw new SchemaInitializationException("intentional lifecycle startup failure");
    }
  }

  @Singleton
  @Requires(property = "consumer.blocking", value = "true")
  public static class BlockingJob {
    static CountDownLatch entered;
    static CountDownLatch release;
    static final AtomicBoolean interrupted = new AtomicBoolean();
    private final EntityManager entityManager;

    public BlockingJob(EntityManager entityManager) {
      this.entityManager = entityManager;
    }

    @Transactional
    public void run() {
      entered.countDown();
      try {
        if (!release.await(30, TimeUnit.SECONDS))
          throw new IllegalStateException("Blocked job not released");
      } catch (InterruptedException failure) {
        interrupted.set(true);
        Thread.currentThread().interrupt();
        throw new IllegalStateException(failure);
      }
      entityManager
          .createNativeQuery(
              "insert into consumer_record (id, state) values ('drained', 'executed')")
          .executeUpdate();
    }
  }
}
