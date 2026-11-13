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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.TransactionalJob;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.spi.JobStore;

/** Uses the normal consumer datasource and Ratchet-selected Spring transaction manager. */
class TransactionalJobTest {
  @Test
  void successCommitsApplicationRowAndJob() throws Exception {
    verify("success");
  }

  @Test
  void firstBodyFailureRollsBackBeforeRetry() throws Exception {
    verify("retry");
  }

  @Test
  void cancellationRollsBackApplicationWrite() throws Exception {
    verify("cancel");
  }

  @Test
  void durableSuccessCommitsOutboxWithApplicationAndJobRows() throws Exception {
    verify("durable");
  }

  private void verify(String scenario) throws Exception {
    try (var database = SqlDatabase.start();
        var context =
            new SpringApplicationBuilder(ConsumerApplication.class, JobConfiguration.class)
                .properties(database.properties())
                // Keep recovery from redelivering while the synchronous listener observes the
                // outbox.
                .properties(
                    "ratchet.events.durable=" + scenario.equals("durable"),
                    "ratchet.node.orphan-grace-seconds=60")
                .run()) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var store = context.getBean(JobStore.class);
      var jobs = context.getBean(TransactionalJobs.class);
      var events = context.getBean(TerminalObserver.class);
      // Independent autocommit reads cannot see the job transaction's uncommitted application row.
      var properties = database.properties();
      var probe =
          new JdbcTemplate(
              new DriverManagerDataSource(
                  (String) properties.get("spring.datasource.url"),
                  (String) properties.get("spring.datasource.username"),
                  (String) properties.get("spring.datasource.password")));
      events.database = database;
      events.probe = probe;
      String row = "tx-" + UUID.randomUUID();
      events.row = row;
      UUID job =
          scheduler
              .enqueue(() -> jobs.write(row, scenario))
              .withMaxRetries(1)
              .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
              .submit()
              .id();
      try {
        if (scenario.equals("durable")) {
          assertThat(jobs.written.await(30, TimeUnit.SECONDS)).isTrue();
          assertThat(count(probe, row)).isZero();
          assertThat(outboxCount(probe, database, job)).isZero();
          assertThat(store.findById(job).orElseThrow().getStatus()).isEqualTo(JobStatus.RUNNING);
          jobs.release.countDown();
        }
        if (scenario.equals("cancel")) {
          assertThat(jobs.written.await(30, TimeUnit.SECONDS)).isTrue();
          assertThat(count(probe, row)).isZero();
          assertThat(scheduler.cancelJob(job)).isTrue();
          jobs.release.countDown();
          assertThat(jobs.rolledBack.await(30, TimeUnit.SECONDS)).isTrue();
        }
        JobStatus expected = scenario.equals("cancel") ? JobStatus.CANCELED : JobStatus.SUCCEEDED;
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> {
                  assertThat(store.findById(job).orElseThrow().getStatus()).isEqualTo(expected);
                  assertThat(count(probe, row)).isEqualTo(scenario.equals("cancel") ? 0 : 1);
                });
        assertThat(jobs.activeTransaction).isTrue();
        if (scenario.equals("retry")) {
          assertThat(jobs.invocations.get()).isGreaterThanOrEqualTo(2);
          assertThat(jobs.rolledBack.getCount()).isZero();
        }
        if (scenario.equals("durable")) {
          await()
              .atMost(Duration.ofSeconds(30))
              .untilAsserted(
                  () ->
                      assertThat(events.observations)
                          .singleElement()
                          .satisfies(
                              observation -> {
                                assertThat(observation.job()).isEqualTo(job);
                                assertThat(observation.status()).isEqualTo("SUCCEEDED");
                                assertThat(observation.businessRows()).isEqualTo(1);
                                assertThat(observation.outboxRows()).isEqualTo(1);
                              }));
          await()
              .atMost(Duration.ofSeconds(30))
              .untilAsserted(() -> assertThat(outboxCount(probe, database, job)).isZero());
          await()
              .during(Duration.ofSeconds(2))
              .atMost(Duration.ofSeconds(5))
              .untilAsserted(() -> assertThat(events.observations).hasSize(1));
        }
      } finally {
        jobs.release.countDown();
      }
    }
  }

  private static int count(JdbcTemplate jdbc, String row) {
    return jdbc.queryForObject(
        "select count(*) from consumer_record where id = ?", Integer.class, row);
  }

  private static int outboxCount(JdbcTemplate jdbc, SqlDatabase database, UUID job) {
    return jdbc.queryForObject(
        "select count(*) from scheduler_event_outbox where " + database.jobIdPredicate(),
        Integer.class,
        database.queryId(job));
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class JobConfiguration {
    @Bean
    TransactionalJobs transactionalJobs(JdbcTemplate jdbc) {
      return new TransactionalJobs(jdbc);
    }

    @Bean
    TerminalObserver terminalObserver() {
      return new TerminalObserver();
    }
  }

  public static class TransactionalJobs {
    private final JdbcTemplate jdbc;
    final AtomicInteger invocations = new AtomicInteger();
    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch rolledBack = new CountDownLatch(1);
    volatile boolean activeTransaction;

    TransactionalJobs(JdbcTemplate jdbc) {
      this.jdbc = jdbc;
    }

    @TransactionalJob
    public void write(String row, String scenario) throws InterruptedException {
      activeTransaction = TransactionSynchronizationManager.isActualTransactionActive();
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
              if (status == STATUS_ROLLED_BACK) rolledBack.countDown();
            }
          });
      int invocation = invocations.incrementAndGet();
      jdbc.update("insert into consumer_record (id, state) values (?, ?)", row, "transactional");
      if (scenario.equals("retry") && invocation == 1) {
        throw new IllegalStateException("First application write must roll back");
      }
      if (scenario.equals("cancel") || scenario.equals("durable")) {
        written.countDown();
        if (!release.await(30, TimeUnit.SECONDS))
          throw new IllegalStateException("Release timed out");
      }
    }
  }

  record Observation(UUID job, String status, int businessRows, int outboxRows) {}

  public static class TerminalObserver {
    volatile SqlDatabase database;
    volatile JdbcTemplate probe;
    volatile String row;
    final List<Observation> observations = new CopyOnWriteArrayList<>();

    @EventListener
    public void completed(JobCompletedEvent event) {
      if (probe == null || row == null) return;
      // Synchronous delivery precedes acknowledgement/deletion. Independent reads prove all
      // three rows have committed by the time the terminal event is published.
      UUID job = event.getJobId();
      observations.add(
          new Observation(
              job,
              probe.queryForObject(
                  "select terminal_status from scheduler_job where " + database.jobIdPredicate(),
                  String.class,
                  database.queryId(job)),
              count(probe, row),
              outboxCount(probe, database, job)));
    }
  }
}
