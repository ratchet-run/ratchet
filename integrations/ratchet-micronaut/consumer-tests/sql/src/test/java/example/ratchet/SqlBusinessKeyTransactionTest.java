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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.BusinessKeyLookup;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.consumer.sql.SqlAccess;
import run.ratchet.consumer.sql.SqlApplication;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.ri.security.PermitAllJobAuthorizationPolicy;

/**
 * What a duplicate business key does to the caller's own Micronaut transaction. The caller writes
 * an application row, submits a job whose business key is taken, catches the exception and commits.
 */
class SqlBusinessKeyTransactionTest {
  private static final Duration LATER = Duration.ofHours(1);

  /** Midnight on 1 January: the recurring holder never fires during a test run. */
  private static final String YEARLY = "0 0 0 1 1 ?";

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
  void plainDuplicateLeavesTheCallerTransactionCommittable() {
    try (var context = SqlApplication.start(database)) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var jobs = context.getBean(ConsumerJobs.class);
      var jdbc = new SqlAccess(context);
      String key = id("plain-key");
      String own = id("plain-own");
      scheduler.schedule(LATER, () -> jobs.complete("holder")).withBusinessKey(key).submit();

      Observation observed = submitDuplicateInCallerTransaction(context, scheduler, jobs, key, own);

      assertSoftly(
          soft -> {
            soft.assertThat(observed.rollbackOnlyAfterCatch())
                .as("a pre-check duplicate must leave the caller transaction usable")
                .isFalse();
            soft.assertThat(observed.commitFailure()).as("commit failure").isNull();
            soft.assertThat(count(jdbc, own)).as("the caller's own row").isEqualTo(1);
          });
    }
  }

  @Test
  void recurringHolderLeavesTheCallerTransactionCommittable() {
    try (var context = SqlApplication.start(database)) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var jobs = context.getBean(ConsumerJobs.class);
      var jdbc = new SqlAccess(context);
      String key = id("race-key");
      String own = id("race-own");
      var master =
          scheduler
              .scheduleRecurring(YEARLY, ZoneOffset.UTC, () -> jobs.complete("recurring"))
              .withBusinessKey(key)
              .submit();

      var expected = new BusinessKeyHolder(master.id(), JobType.RECURRING, JobStatus.PENDING);
      var lookup = context.getBean(BusinessKeyLookup.class);
      assertThat(scheduler.findBusinessKeyHolder(key)).contains(expected);
      assertThat(lookup.findBusinessKeyHolder(key)).contains(expected);

      Observation observed = submitDuplicateInCallerTransaction(context, scheduler, jobs, key, own);

      assertSoftly(
          soft -> {
            soft.assertThat(observed.rollbackOnlyAfterCatch()).as("rollback-only").isFalse();
            soft.assertThat(observed.commitFailure()).as("commit failure").isNull();
            soft.assertThat(count(jdbc, own)).as("the caller's own row").isEqualTo(1);
            soft.assertThat(
                    jdbc.queryForObject(
                        "select count(*) from scheduler_job where business_key = ?",
                        Integer.class,
                        key))
                .as("the loser's job rows")
                .isZero();
          });
    }
  }

  @Test
  void idempotencyInsertCollisionReturnsOriginalAndCommitsCallerWrite() {
    try (var context = SqlApplication.start(database, "consumer.idempotency-race=true")) {
      var scheduler = context.getBean(JobSchedulerService.class);
      var jobs = context.getBean(ConsumerJobs.class);
      var policy = context.getBean(RacePolicy.class);
      var jdbc = new SqlAccess(context);
      var transactions = jdbc;
      var worker = Executors.newSingleThreadExecutor();
      String key = UUID.randomUUID().toString();
      String own = id("idempotency-own");
      String businessKey = id("unused-business");
      var original = new AtomicReference<JobHandle>();
      try {
        transactions.transaction(
            status -> {
              jdbc.update("insert into consumer_record (id, state) values (?, ?)", own, "own");
              // Authorization runs after the loser's pre-check. Commit a competing submission
              // now so this call must exercise the store conflict and fresh-transaction lookup.
              policy.beforeCreate.set(
                  () -> {
                    try {
                      original.set(
                          worker
                              .submit(
                                  () ->
                                      scheduler
                                          .schedule(LATER, () -> jobs.complete("winner"))
                                          .withIdempotencyKey(key)
                                          .submit())
                              .get(30, TimeUnit.SECONDS));
                    } catch (Exception failure) {
                      throw new IllegalStateException("Winner did not commit", failure);
                    }
                  });
              JobHandle handle =
                  scheduler
                      .schedule(LATER, () -> jobs.complete("loser"))
                      .withIdempotencyKey(key)
                      .withBusinessKey(businessKey)
                      .submit();
              assertThat(handle.id()).isEqualTo(original.get().id());
              assertThat(status.isRollbackOnly()).isFalse();
              assertThat(jdbc.queryForObject("select 1", Integer.class)).isEqualTo(1);
            });
        assertThat(count(jdbc, own)).isEqualTo(1);
        assertThat(scheduler.findBusinessKeyHolder(businessKey)).isEmpty();
      } finally {
        policy.beforeCreate.set(null);
        worker.shutdownNow();
      }
    }
  }

  @Factory
  @Requires(property = "consumer.idempotency-race", value = "true")
  static class RaceConfiguration {
    @Singleton
    RacePolicy racePolicy() {
      return new RacePolicy();
    }
  }

  public static class RacePolicy extends PermitAllJobAuthorizationPolicy {
    final AtomicReference<Runnable> beforeCreate = new AtomicReference<>();

    @Override
    public void checkCreate(UUID jobId, String principal) {
      Runnable action = beforeCreate.getAndSet(null);
      if (action != null) action.run();
    }
  }

  private static Observation submitDuplicateInCallerTransaction(
      ApplicationContext context,
      JobSchedulerService scheduler,
      ConsumerJobs jobs,
      String key,
      String own) {
    var jdbc = new SqlAccess(context);
    var expected = scheduler.findBusinessKeyHolder(key);
    var lookup = context.getBean(BusinessKeyLookup.class);
    var rollbackOnly = new AtomicBoolean();
    Throwable commitFailure =
        catchThrowable(
            () ->
                jdbc.transaction(
                    status -> {
                      assertThat(scheduler.findBusinessKeyHolder(key)).isEqualTo(expected);
                      assertThat(lookup.findBusinessKeyHolder(key)).isEqualTo(expected);
                      assertThat(lookup.findBusinessKeyHolder(id("unheld"))).isEmpty();
                      assertThatThrownBy(() -> lookup.findBusinessKeyHolder("x".repeat(256)))
                          .isInstanceOf(IllegalArgumentException.class);
                      jdbc.update(
                          "insert into consumer_record (id, state) values (?, ?)", own, "own");
                      assertThatThrownBy(
                              () ->
                                  scheduler
                                      .schedule(LATER, () -> jobs.complete("duplicate"))
                                      .withBusinessKey(key)
                                      .submit())
                          .isInstanceOf(DuplicateBusinessKeyException.class);
                      rollbackOnly.set(status.isRollbackOnly());
                    }));
    // Surface an assertion that failed inside the callback; a null failure means it committed.
    if (commitFailure instanceof AssertionError assertionFailure) {
      throw assertionFailure;
    }
    return new Observation(rollbackOnly.get(), commitFailure);
  }

  private record Observation(boolean rollbackOnlyAfterCatch, Throwable commitFailure) {}

  private static int count(SqlAccess jdbc, String id) {
    return jdbc.queryForObject(
        "select count(*) from consumer_record where id = ?", Integer.class, id);
  }

  private static String id(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }
}
