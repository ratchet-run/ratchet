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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import io.micronaut.transaction.TransactionOperations;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.NodeIdentity;
import run.ratchet.consumer.sql.SqlAccess;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobWakeupHint;

class SqlTransactionsTest {
  private static SqlDatabase database;

  @BeforeAll
  static void startDatabase() {
    database = SqlDatabase.start();
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) database.close();
  }

  @Test
  void requiresNewJobAndApplicationRowCommitWhenOuterTransactionRollsBack() {
    String outerId = id("outer");
    String innerId = id("inner");
    try (var context = application()) {
      var scenarios = context.getBean(ConsumerTransactionScenarios.class);
      var jdbc = new SqlAccess(context);
      JobHandle handle = scenarios.requiresNewCommitsWhileOuterRollsBack(outerId, innerId);

      assertThat(count(jdbc, outerId)).isZero();
      assertThat(countJob(jdbc, handle)).isEqualTo(1);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(state(jdbc, innerId)).isEqualTo("executed"));
    }
  }

  @Test
  void notSupportedSubmissionUsesItsOwnTransactionAndExecutes() {
    String id = id("not-supported");
    try (var context = application()) {
      var scenarios = context.getBean(ConsumerTransactionScenarios.class);
      var jdbc = new SqlAccess(context);
      JobHandle handle = scenarios.notSupportedSubmitsInItsOwnTransaction(id);

      assertThat(countJob(jdbc, handle)).isEqualTo(1);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(state(jdbc, id)).isEqualTo("executed"));
    }
  }

  @Test
  void rollbackOnlyRemovesTheApplicationRowAndSubmittedJob() {
    String id = id("rollback-only");
    try (var context = application()) {
      var scenarios = context.getBean(ConsumerTransactionScenarios.class);
      var jdbc = new SqlAccess(context);
      JobHandle handle = scenarios.rollbackOnlySuppressesApplicationAndJob(id);

      assertThat(count(jdbc, id)).isZero();
      assertThat(countJob(jdbc, handle)).isZero();
    }
  }

  @Test
  void jakartaCheckedExceptionWithRollbackOnRollsBackApplicationAndJob() {
    String id = id("checked");
    try (var context = application()) {
      var scenarios = context.getBean(ConsumerTransactionScenarios.class);
      var jdbc = new SqlAccess(context);
      int before = jdbc.queryForObject("select count(*) from scheduler_job", Integer.class);

      assertThatThrownBy(() -> scenarios.checkedExceptionRollsBack(id))
          .isInstanceOf(ConsumerTransactionScenarios.ConsumerCheckedException.class);

      assertThat(count(jdbc, id)).isZero();
      assertThat(jdbc.queryForObject("select count(*) from scheduler_job", Integer.class))
          .isEqualTo(before);
    }
  }

  @Test
  void batchChildrenExecuteAndCompleteAfterTheParentFollowupTransaction() {
    String first = id("batch-first");
    String second = id("batch-second");
    try (var context = application()) {
      var service = context.getBean(ConsumerService.class);
      var jdbc = new SqlAccess(context);
      JobHandle parent = service.submitBatch(first, second);

      assertThat(countJob(jdbc, parent)).isEqualTo(1);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(state(jdbc, first)).isEqualTo("executed");
                assertThat(state(jdbc, second)).isEqualTo("executed");
                assertThat(
                        jdbc.queryForObject(
                            "select terminal_status from scheduler_job where "
                                + database.jobIdPredicate(),
                            String.class,
                            database.queryId(parent.id())))
                    .isEqualTo("SUCCEEDED");
              });
    }
  }

  @Test
  void failedAfterCommitRegistrationSuppressesTheSynchronousWakeupWhileKeepingSqlDurable() {
    String id = id("registration-failed");
    try (var context = SqlApplication.start(database, "consumer.failed-registrar=true")) {
      var service = context.getBean(ConsumerService.class);
      var jdbc = new SqlAccess(context);
      var probe = context.getBean(FailedAfterCommitProbe.class);
      var coordinator = context.getBean(RecordingClusterCoordinator.class);
      JobHandle handle = service.submit(id);

      assertThat(count(jdbc, id)).isEqualTo(1);
      assertThat(countJob(jdbc, handle)).isEqualTo(1);
      assertThat(probe.registrations()).isEqualTo(1);
      assertThat(coordinator.notifications()).isZero();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(state(jdbc, id)).isEqualTo("executed");
                assertThat(
                        jdbc.queryForObject(
                            "select terminal_status from scheduler_job where "
                                + database.jobIdPredicate(),
                            String.class,
                            database.queryId(handle.id())))
                    .isEqualTo("SUCCEEDED");
              });
    }
  }

  @Test
  void configuredThreadTargetsRetainTransactionAdviceOnEachSupportedJavaRuntime() {
    boolean virtualAvailable = Runtime.version().feature() >= 21;
    String defaultId = id("thread-default");
    String virtualId = id("thread-virtual");
    String platformId = id("thread-platform");
    try (var context =
        SqlApplication.start(
            database, "consumer.thread-targets=true", "micronaut.executors.default.virtual=true")) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var jobs = context.getBean(ThreadProbeJobs.class);
      var jdbc = new SqlAccess(context);
      scheduler.enqueue(() -> jobs.record(defaultId)).submit();
      scheduler.enqueue(() -> jobs.record(virtualId)).virtual().submit();
      scheduler.enqueue(() -> jobs.record(platformId)).platform().submit();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(jobs.observation(defaultId))
                    .isEqualTo(new ThreadObservation(virtualAvailable, true));
                assertThat(jobs.observation(virtualId))
                    .isEqualTo(new ThreadObservation(virtualAvailable, true));
                assertThat(jobs.observation(platformId))
                    .isEqualTo(new ThreadObservation(false, true));
                for (String id : new String[] {defaultId, virtualId, platformId})
                  assertThat(count(jdbc, id)).isEqualTo(1);
              });
    }
  }

  record ThreadObservation(boolean virtual, boolean transactional) {}

  @Singleton
  @Requires(property = "consumer.thread-targets", value = "true")
  public static class ThreadProbeJobs {
    private final EntityManager entityManager;
    private final TransactionOperations<EntityManager> transactions;
    private final Map<String, ThreadObservation> observations = new ConcurrentHashMap<>();

    public ThreadProbeJobs(
        EntityManager entityManager, TransactionOperations<EntityManager> transactions) {
      this.entityManager = entityManager;
      this.transactions = transactions;
    }

    @Transactional
    public void record(String id) throws ReflectiveOperationException {
      boolean virtual =
          Runtime.version().feature() >= 21
              && (Boolean) Thread.class.getMethod("isVirtual").invoke(Thread.currentThread());
      entityManager.persist(new ConsumerRecord(id, "executed"));
      observations.put(
          id, new ThreadObservation(virtual, transactions.findTransactionStatus().isPresent()));
    }

    ThreadObservation observation(String id) {
      return observations.get(id);
    }
  }

  @Factory
  @Requires(property = "consumer.thread-targets", value = "true")
  static class ThreadProbeConfiguration {
    @Singleton
    @Bean(preDestroy = "close")
    ThreadExecutors executors() {
      return new ThreadExecutors();
    }
  }

  public static final class ThreadExecutors implements ExecutorProvider, AutoCloseable {
    private final ExecutorService platform = Executors.newCachedThreadPool();
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2);
    private final ExecutorService virtual;

    public ThreadExecutors() {
      try {
        virtual =
            Runtime.version().feature() >= 21
                ? (ExecutorService)
                    Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null)
                : null;
      } catch (ReflectiveOperationException failure) {
        throw new IllegalStateException(failure);
      }
    }

    public ExecutorService getJobExecutor() {
      return platform;
    }

    public Optional<ExecutorService> getJobExecutor(String target) {
      return ExecutorTargets.VIRTUAL.equals(target)
          ? Optional.ofNullable(virtual)
          : ExecutorProvider.super.getJobExecutor(target);
    }

    public ScheduledExecutorService getScheduledExecutor() {
      return timers;
    }

    public void close() {
      platform.shutdownNow();
      timers.shutdownNow();
      if (virtual != null) virtual.shutdownNow();
    }
  }

  private static ApplicationContext application() {
    return SqlApplication.start(database);
  }

  private static int count(SqlAccess jdbc, String id) {
    return jdbc.queryForObject(
        "select count(*) from consumer_record where id = ?", Integer.class, id);
  }

  private static int countJob(SqlAccess jdbc, JobHandle handle) {
    return jdbc.queryForObject(
        "select count(*) from scheduler_job where " + database.jobIdPredicate(),
        Integer.class,
        database.queryId(handle.id()));
  }

  private static String state(SqlAccess jdbc, String id) {
    return jdbc.queryForObject("select state from consumer_record where id = ?", String.class, id);
  }

  private static String id(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  @Factory
  @Requires(property = "consumer.failed-registrar", value = "true")
  static class FailedRegistrarConfiguration {
    @Singleton
    @Primary
    FailedAfterCommitProbe failedAfterCommitProbe() {
      return new FailedAfterCommitProbe();
    }

    @Singleton
    @Primary
    AfterCommitRegistrar failedAfterCommitRegistrar(FailedAfterCommitProbe probe) {
      return action -> {
        probe.recordRegistration();
        return AfterCommitRegistrar.Result.REGISTRATION_FAILED;
      };
    }

    @Singleton
    @Primary
    RecordingClusterCoordinator recordingClusterCoordinator() {
      return new RecordingClusterCoordinator();
    }
  }

  static final class FailedAfterCommitProbe {
    private final AtomicInteger registrations = new AtomicInteger();

    void recordRegistration() {
      registrations.incrementAndGet();
    }

    int registrations() {
      return registrations.get();
    }
  }

  static final class RecordingClusterCoordinator implements ClusterCoordinator {
    private final AtomicInteger notifications = new AtomicInteger();

    @Override
    public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
      notifications.incrementAndGet();
    }

    @Override
    public void registerWakeupListener(Consumer<JobWakeupHint> listener) {}

    @Override
    public void close() {}

    int notifications() {
      return notifications.get();
    }
  }
}
