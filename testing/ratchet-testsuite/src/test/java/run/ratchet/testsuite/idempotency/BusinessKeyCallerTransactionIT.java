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
package run.ratchet.testsuite.idempotency;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * What a duplicate business key does to the caller's own JTA transaction.
 *
 * <p>Each test opens a caller transaction, writes something of its own (a key-less job), submits a
 * job whose business key is taken, catches the exception and tries to commit.
 *
 * <ul>
 *   <li>Plain duplicate: an active one-off job holds the key, so the pre-check throws before any
 *       insert. No statement fails, so the caller should be able to commit its own write.
 *   <li>Lost race: a recurring master holds the key. The pre-check reads only queue owners, so the
 *       submit reaches the insert path and its reservation insert fails, as the loser of a real
 *       race does. That transaction cannot commit.
 * </ul>
 */
@EnabledIfSystemProperty(
    named = "ratchet.test.db.type",
    matches = "mysql|postgresql|oracle|sqlserver")
class BusinessKeyCallerTransactionIT extends BaseRatchetIT {

  private static final Duration LATER = Duration.ofHours(1);

  /** Midnight on 1 January: the recurring holder never fires during a test run. */
  private static final String YEARLY = "0 0 0 1 1 ?";

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private JobQueryService jobQueryService;

  @Inject private UserTransaction utx;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @AfterEach
  void endCallerTransaction() throws Exception {
    if (utx.getStatus() != Status.STATUS_NO_TRANSACTION) {
      utx.rollback();
    }
  }

  @Test
  void plainDuplicate_callerCatchesItAndCommitsItsOwnWrite() throws Exception {
    String key = uniqueKey();
    JobHandle holder = jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key).submit();

    utx.begin();
    JobHandle ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
    assertThrows(
        DuplicateBusinessKeyException.class,
        () -> jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key).submit());
    Outcome outcome = commitCallerTransaction();

    assertAll(
        () ->
            assertEquals(
                "STATUS_ACTIVE",
                outcome.statusAfterCatch(),
                "a pre-check duplicate must leave the caller transaction usable: " + outcome),
        () -> assertNull(outcome.commitFailure(), "the caller must be able to commit: " + outcome),
        () ->
            assertTrue(
                jobCrudStore.findById(ownWrite.id()).isPresent(),
                "the caller's own write must survive the commit: " + outcome),
        () ->
            assertEquals(
                holder.id(),
                jobCrudStore.findActiveByBusinessKey(key).map(JobEntity::getId).orElse(null),
                "the original holder keeps the key"));
  }

  @Test
  void lostRace_generatedIdempotencyKey_leavesCallerTransactionRollbackOnly() throws Exception {
    String key = uniqueKey();
    holdWithRecurringMaster(key);

    utx.begin();
    JobHandle ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
    DuplicateBusinessKeyException thrown =
        assertThrows(
            DuplicateBusinessKeyException.class,
            () -> jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key).submit());
    Outcome outcome = commitCallerTransaction();

    assertAll(
        () ->
            assertInstanceOf(
                BusinessKeyReservationConflictException.class,
                thrown.getCause(),
                "the duplicate came from the failed reservation insert"),
        () ->
            assertEquals("STATUS_MARKED_ROLLBACK", outcome.statusAfterCatch(), outcome.toString()),
        () -> assertEquals("RollbackException", outcome.commitFailure(), outcome.toString()),
        () ->
            assertFalse(
                jobCrudStore.findById(ownWrite.id()).isPresent(),
                "the caller's own write is rolled back with the failed submit: " + outcome),
        () ->
            assertEquals(
                0,
                jobsWithBusinessKey(key),
                "the loser's job rows must not survive without a reservation: " + outcome));
  }

  @Test
  void lostRace_customIdempotencyKey_leavesCallerTransactionRollbackOnly() throws Exception {
    String key = uniqueKey();
    holdWithRecurringMaster(key);

    utx.begin();
    JobHandle ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                jobService
                    .schedule(LATER, SimpleJob::execute)
                    .withIdempotencyKey(UUID.randomUUID().toString())
                    .withBusinessKey(key)
                    .submit());
    Outcome outcome = commitCallerTransaction();

    assertAll(
        // The fresh-transaction lookup reads only queue owners, so it cannot resolve a recurring
        // holder and the store's conflict is rethrown.
        () ->
            assertInstanceOf(
                RatchetTransientStoreException.class,
                thrown,
                "unresolved reservation conflict: " + thrown),
        () ->
            assertEquals("STATUS_MARKED_ROLLBACK", outcome.statusAfterCatch(), outcome.toString()),
        () -> assertEquals("RollbackException", outcome.commitFailure(), outcome.toString()),
        () ->
            assertFalse(
                jobCrudStore.findById(ownWrite.id()).isPresent(),
                "the caller's own write is rolled back with the failed submit: " + outcome),
        () ->
            assertEquals(
                0,
                jobsWithBusinessKey(key),
                "the loser's job rows must not survive without a reservation: " + outcome));
  }

  private void holdWithRecurringMaster(String key) {
    jobService
        .scheduleRecurring(YEARLY, ZoneOffset.UTC, SimpleJob::execute)
        .withBusinessKey(key)
        .submit();
    assertTrue(
        jobCrudStore.findActiveByBusinessKey(key).isEmpty(),
        "precondition: the active-holder pre-check does not see a recurring holder");
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

  private int jobsWithBusinessKey(String key) {
    return jobQueryService
        .findJobs(JobFilter.builder().businessKey(key).build(), 10, 0)
        .items()
        .size();
  }

  private static String uniqueKey() {
    return "caller-tx-" + UUID.randomUUID();
  }

  private static String statusName(int status) {
    return switch (status) {
      case Status.STATUS_ACTIVE -> "STATUS_ACTIVE";
      case Status.STATUS_MARKED_ROLLBACK -> "STATUS_MARKED_ROLLBACK";
      case Status.STATUS_NO_TRANSACTION -> "STATUS_NO_TRANSACTION";
      case Status.STATUS_ROLLEDBACK -> "STATUS_ROLLEDBACK";
      default -> "STATUS_" + status;
    };
  }

  private record Outcome(String statusAfterCatch, String commitFailure) {}
}
