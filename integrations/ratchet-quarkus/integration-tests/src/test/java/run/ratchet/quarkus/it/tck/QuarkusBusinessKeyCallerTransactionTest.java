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
package run.ratchet.quarkus.it.tck;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.BusinessKeyLookup;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.ri.security.PermitAllJobAuthorizationPolicy;
import run.ratchet.tck.api.TckJobs;

/**
 * What a duplicate business key does to the caller's own JTA transaction on Quarkus. The caller
 * submits a key-less job as its own write, submits a job whose business key is taken, catches the
 * exception and commits.
 */
@QuarkusTest
@TestProfile(QuarkusBusinessKeyCallerTransactionTest.RaceProfile.class)
class QuarkusBusinessKeyCallerTransactionTest {
  private static final Duration LATER = Duration.ofHours(1);

  /** Midnight on 1 January: the recurring holder never fires during a test run. */
  private static final String YEARLY = "0 0 0 1 1 ?";

  @Inject QuarkusRatchetTckRuntime runtime;
  @Inject JobQueryService queries;
  @Inject UserTransaction utx;

  @BeforeEach
  void clearBeforeEach() {
    runtime.clear();
    TckJobs.resetAll();
  }

  @AfterEach
  void endCallerTransaction() throws Exception {
    if (utx.getStatus() != Status.STATUS_NO_TRANSACTION) utx.rollback();
    runtime.clear();
  }

  @Test
  void plainDuplicate_callerCatchesItAndCommitsItsOwnWrite() throws Exception {
    JobSchedulerService scheduler = runtime.scheduler();
    String key = uniqueKey();
    scheduler.schedule(LATER, TckJobs::noop).withBusinessKey(key).submit();

    utx.begin();
    JobHandle ownWrite = scheduler.schedule(LATER, TckJobs::noop).submit();
    assertThrows(
        DuplicateBusinessKeyException.class,
        () -> scheduler.schedule(LATER, TckJobs::noop).withBusinessKey(key).submit());
    Outcome outcome = commitCallerTransaction();

    assertAll(
        () -> assertEquals("STATUS_ACTIVE", outcome.statusAfterCatch(), outcome.toString()),
        () -> assertNull(outcome.commitFailure(), outcome.toString()),
        () ->
            assertTrue(
                queries.getJobDetail(ownWrite.id()).isPresent(),
                "the caller's own write must survive the commit: " + outcome));
  }

  @Test
  void recurringHolder_callerCommitsItsOwnWrite() throws Exception {
    JobSchedulerService scheduler = runtime.scheduler();
    String key = uniqueKey();
    var master =
        scheduler
            .scheduleRecurring(YEARLY, ZoneOffset.UTC, TckJobs::noop)
            .withBusinessKey(key)
            .submit();

    var expected = new BusinessKeyHolder(master.id(), JobType.RECURRING, JobStatus.PENDING);
    var lookup = CDI.current().select(BusinessKeyLookup.class).get();
    assertEquals(expected, scheduler.findBusinessKeyHolder(key).orElseThrow());
    assertEquals(expected, lookup.findBusinessKeyHolder(key).orElseThrow());

    utx.begin();
    assertEquals(expected, scheduler.findBusinessKeyHolder(key).orElseThrow());
    assertEquals(expected, lookup.findBusinessKeyHolder(key).orElseThrow());
    assertTrue(lookup.findBusinessKeyHolder(uniqueKey()).isEmpty());
    assertThrows(
        IllegalArgumentException.class, () -> lookup.findBusinessKeyHolder("x".repeat(256)));
    JobHandle ownWrite = scheduler.schedule(LATER, TckJobs::noop).submit();
    assertThrows(
        DuplicateBusinessKeyException.class,
        () -> scheduler.schedule(LATER, TckJobs::noop).withBusinessKey(key).submit());
    Outcome outcome = commitCallerTransaction();

    assertAll(
        () -> assertEquals("STATUS_ACTIVE", outcome.statusAfterCatch(), outcome.toString()),
        () -> assertNull(outcome.commitFailure(), outcome.toString()),
        () ->
            assertTrue(
                queries.getJobDetail(ownWrite.id()).isPresent(),
                "the caller's own write survives the commit: " + outcome),
        () ->
            assertEquals(
                0,
                queries
                    .findJobs(JobFilter.builder().businessKey(key).build(), 10, 0)
                    .items()
                    .size(),
                "the loser's job rows must not survive without a reservation: " + outcome));
  }

  @Test
  void idempotencyInsertCollisionReturnsOriginalAndCommitsCallerWrite() throws Exception {
    JobSchedulerService scheduler = runtime.scheduler();
    RacePolicy policy = CDI.current().select(RacePolicy.class).get();
    var worker = Executors.newSingleThreadExecutor();
    var original = new AtomicReference<JobHandle>();
    String key = UUID.randomUUID().toString();
    String businessKey = uniqueKey();
    try {
      utx.begin();
      JobHandle ownWrite = scheduler.schedule(LATER, TckJobs::noop).submit();
      policy.setBeforeCreate(
          () -> {
            try {
              original.set(
                  worker
                      .submit(
                          () ->
                              scheduler
                                  .schedule(LATER, TckJobs::noop)
                                  .withIdempotencyKey(key)
                                  .submit())
                      .get(30, TimeUnit.SECONDS));
            } catch (Exception failure) {
              throw new IllegalStateException("Winner did not commit", failure);
            }
          });
      JobHandle replay =
          scheduler
              .schedule(LATER, TckJobs::noop)
              .withIdempotencyKey(key)
              .withBusinessKey(businessKey)
              .submit();
      assertEquals(original.get().id(), replay.id());
      Outcome outcome = commitCallerTransaction();
      assertEquals("STATUS_ACTIVE", outcome.statusAfterCatch());
      assertNull(outcome.commitFailure());
      assertTrue(queries.getJobDetail(ownWrite.id()).isPresent());
      assertTrue(scheduler.findBusinessKeyHolder(businessKey).isEmpty());
    } finally {
      policy.setBeforeCreate(null);
      worker.shutdownNow();
    }
  }

  public static class RaceProfile extends QuarkusRatchetTckProfile {
    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(RacePolicy.class);
    }
  }

  @Alternative
  @ApplicationScoped
  public static class RacePolicy extends PermitAllJobAuthorizationPolicy {
    private final AtomicReference<Runnable> beforeCreate = new AtomicReference<>();

    public void setBeforeCreate(Runnable action) {
      beforeCreate.set(action);
    }

    @Override
    public void checkCreate(UUID id, String principal) {
      Runnable action = beforeCreate.getAndSet(null);
      if (action != null) action.run();
    }
  }

  private Outcome commitCallerTransaction() throws SystemException {
    String status = statusName(utx.getStatus());
    String failure = null;
    try {
      utx.commit();
    } catch (Exception e) {
      failure = e.getClass().getSimpleName();
    }
    return new Outcome(status, failure);
  }

  private static String uniqueKey() {
    return "caller-tx-" + UUID.randomUUID();
  }

  private static String statusName(int status) {
    return switch (status) {
      case Status.STATUS_ACTIVE -> "STATUS_ACTIVE";
      case Status.STATUS_MARKED_ROLLBACK -> "STATUS_MARKED_ROLLBACK";
      case Status.STATUS_NO_TRANSACTION -> "STATUS_NO_TRANSACTION";
      default -> "STATUS_" + status;
    };
  }

  private record Outcome(String statusAfterCatch, String commitFailure) {}
}
