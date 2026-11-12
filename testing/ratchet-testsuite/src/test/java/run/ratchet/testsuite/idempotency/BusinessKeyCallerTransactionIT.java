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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.tck.store.JdbcLockWait;
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
 * <p>Both an up-front duplicate and a held reservation leave the caller able to commit.
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
        .addClasses(JdbcLockWait.class, JdbcLockWait.ConnectionFactory.class)
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
  void recurringHolder_generatedIdempotencyKey_callerCommitsItsOwnWrite() throws Exception {
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
                "the duplicate came from the held reservation"),
        () -> assertEquals("STATUS_ACTIVE", outcome.statusAfterCatch(), outcome.toString()),
        () -> assertNull(outcome.commitFailure(), outcome.toString()),
        () ->
            assertTrue(
                jobCrudStore.findById(ownWrite.id()).isPresent(),
                "the caller's own write survives the commit: " + outcome),
        () ->
            assertEquals(
                0,
                jobsWithBusinessKey(key),
                "the loser's job rows must not survive without a reservation: " + outcome));
  }

  @Test
  void recurringHolder_customIdempotencyKey_callerCommitsItsOwnWrite() throws Exception {
    String key = uniqueKey();
    holdWithRecurringMaster(key);

    utx.begin();
    JobHandle ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
    DuplicateBusinessKeyException thrown =
        assertThrows(
            DuplicateBusinessKeyException.class,
            () ->
                jobService
                    .schedule(LATER, SimpleJob::execute)
                    .withIdempotencyKey(UUID.randomUUID().toString())
                    .withBusinessKey(key)
                    .submit());
    Outcome outcome = commitCallerTransaction();

    assertAll(
        () -> assertInstanceOf(BusinessKeyReservationConflictException.class, thrown.getCause()),
        () -> assertEquals("STATUS_ACTIVE", outcome.statusAfterCatch(), outcome.toString()),
        () -> assertNull(outcome.commitFailure(), outcome.toString()),
        () ->
            assertTrue(
                jobCrudStore.findById(ownWrite.id()).isPresent(),
                "the caller's own write survives the commit: " + outcome),
        () ->
            assertEquals(
                0,
                jobsWithBusinessKey(key),
                "the loser's job rows must not survive without a reservation: " + outcome));
  }

  // The race and REPEATABLE READ tests read the datasource by its WildFly JNDI name, and the
  // REPEATABLE READ tests rely on WildFly sharing one connection per transaction. They run on
  // WildFly only. A method-level condition skips on the client; an in-container assumption failure
  // is reported as an error, not a skip.
  static boolean onWildFly() {
    // The client sees the Maven profile. The server JVM does not, but it has its home directory.
    return System.getProperty("testsuite.profile", "").startsWith("wildfly")
        || System.getProperty("jboss.home.dir") != null;
  }

  @Test
  @EnabledIf("onWildFly")
  void concurrentWinnerCommits_loserCommitsItsOwnWrite() throws Exception {
    race(false, false);
  }

  @Test
  @EnabledIf("onWildFly")
  void concurrentWinnerRollsBack_loserSubmitSucceeds() throws Exception {
    race(true, false);
  }

  @Test
  @EnabledIf("onWildFly")
  void concurrentIdempotentReplay_returnsWinnersHandle() throws Exception {
    race(false, true);
  }

  private void race(boolean rollbackWinner, boolean replay) throws Exception {
    String key = uniqueKey();
    String idempotencyKey = UUID.randomUUID().toString();
    CountDownLatch submitted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch attempting = new CountDownLatch(1);
    ManagedExecutorService executor =
        InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
    Future<JobHandle> winner = null;
    Future<RaceOutcome> loser = null;
    try {
      winner =
          executor.submit(
              () -> {
                try {
                  utx.begin();
                  var builder = jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key);
                  if (replay) builder.withIdempotencyKey(idempotencyKey);
                  JobHandle handle = builder.submit();
                  submitted.countDown();
                  assertTrue(release.await(90, TimeUnit.SECONDS), "Winner was not released");
                  if (rollbackWinner) utx.rollback();
                  else utx.commit();
                  return handle;
                } finally {
                  endCallerTransaction();
                }
              });
      assertTrue(submitted.await(30, TimeUnit.SECONDS), "Winner did not submit");
      loser =
          executor.submit(
              () -> {
                try {
                  utx.begin();
                  JobHandle ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
                  var builder = jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key);
                  if (replay) builder.withIdempotencyKey(idempotencyKey);
                  JobHandle handle = null;
                  DuplicateBusinessKeyException duplicate = null;
                  attempting.countDown();
                  try {
                    handle = builder.submit();
                  } catch (DuplicateBusinessKeyException e) {
                    duplicate = e;
                  }
                  assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
                  utx.commit();
                  return new RaceOutcome(ownWrite, handle, duplicate);
                } finally {
                  endCallerTransaction();
                }
              });
      assertTrue(attempting.await(30, TimeUnit.SECONDS), "Loser did not attempt submit");
      awaitBlockedSession();
      Future<RaceOutcome> waitingLoser = loser;
      assertThrows(TimeoutException.class, () -> waitingLoser.get(500, TimeUnit.MILLISECONDS));
      release.countDown();
      JobHandle winningHandle = winner.get(30, TimeUnit.SECONDS);
      RaceOutcome result = loser.get(30, TimeUnit.SECONDS);
      assertTrue(jobCrudStore.findById(result.ownWrite().id()).isPresent());
      assertEquals(1, jobsWithBusinessKey(key));
      if (rollbackWinner) {
        assertNull(result.duplicate());
        assertTrue(jobCrudStore.findById(winningHandle.id()).isEmpty());
        assertEquals(
            result.handle().id(), jobCrudStore.findActiveByBusinessKey(key).orElseThrow().getId());
      } else {
        assertEquals(
            winningHandle.id(), jobCrudStore.findActiveByBusinessKey(key).orElseThrow().getId());
        if (replay) {
          assertNull(result.duplicate());
          assertEquals(winningHandle.id(), result.handle().id());
        } else {
          assertInstanceOf(DuplicateBusinessKeyException.class, result.duplicate());
          assertInstanceOf(
              BusinessKeyReservationConflictException.class, result.duplicate().getCause());
          assertNull(result.handle());
        }
      }
    } finally {
      release.countDown();
      try {
        if (winner != null) winner.get(30, TimeUnit.SECONDS);
      } finally {
        if (loser != null) loser.get(30, TimeUnit.SECONDS);
      }
    }
  }

  /** READ COMMITTED lets the RI pre-check see a holder committed after the caller's first read. */
  @Test
  void readCommitted_lateHolderPreCheck_callerCommitsItsOwnWrite() throws Exception {
    String key = uniqueKey();
    try {
      utx.begin();
      JobHandle ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
      jobQueryService.findJobs(JobFilter.builder().businessKey(key).build(), 10, 0);
      JobHandle holder = commitHolderOnAnotherThread(key, null);
      assertThrows(
          DuplicateBusinessKeyException.class,
          () -> jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key).submit());
      assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
      utx.commit();
      assertTrue(jobCrudStore.findById(ownWrite.id()).isPresent());
      assertEquals(1, jobsWithBusinessKey(key));
      assertEquals(holder.id(), jobCrudStore.findActiveByBusinessKey(key).orElseThrow().getId());
    } finally {
      endCallerTransaction();
    }
  }

  @Test
  @EnabledIf("onWildFly")
  @EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mysql")
  void mysqlRepeatableRead_lateHolderConflict_callerCommitsItsOwnWrite() throws Exception {
    lateHolderUnderRepeatableRead(false);
  }

  @Test
  @EnabledIf("onWildFly")
  @EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mysql")
  void mysqlRepeatableRead_lateHolderReplay_returnsHolderAndCommitsOwnWrite() throws Exception {
    lateHolderUnderRepeatableRead(true);
  }

  private void lateHolderUnderRepeatableRead(boolean replay) throws Exception {
    String key = uniqueKey();
    String idempotencyKey = UUID.randomUUID().toString();
    try {
      utx.begin();
      JobHandle ownWrite;
      JobHandle holder;
      try (var connection = dataSource().getConnection()) {
        // This must be the first statement on the caller's shared managed connection.
        try (var statement = connection.createStatement()) {
          statement.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
          try (var snapshot = statement.executeQuery("SELECT COUNT(*) FROM scheduler_job")) {
            assertTrue(snapshot.next(), "The consistent read must establish the caller's snapshot");
          }
        }
        ownWrite = jobService.schedule(LATER, SimpleJob::execute).submit();
        holder = commitHolderOnAnotherThread(key, idempotencyKey);
        try (var statement =
            connection.prepareStatement(
                "SELECT COUNT(*) FROM scheduler_business_key_reservation WHERE business_key = ?")) {
          statement.setString(1, key);
          try (var snapshot = statement.executeQuery()) {
            assertTrue(snapshot.next());
            assertEquals(
                0, snapshot.getInt(1), "The caller's snapshot must not see the late holder");
          }
        }
        var builder = jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key);
        if (replay) {
          JobHandle handle = builder.withIdempotencyKey(idempotencyKey).submit();
          assertEquals(holder.id(), handle.id(), "The fresh transaction must find the holder");
        } else {
          DuplicateBusinessKeyException thrown =
              assertThrows(DuplicateBusinessKeyException.class, builder::submit);
          assertInstanceOf(
              BusinessKeyReservationConflictException.class,
              thrown.getCause(),
              "The store must join the caller's REPEATABLE READ session and hit the reservation");
        }
        assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
      }
      // Close the managed JDBC handle before committing the caller's transaction.
      utx.commit();
      assertTrue(jobCrudStore.findById(ownWrite.id()).isPresent());
      assertEquals(1, jobsWithBusinessKey(key));
      assertEquals(holder.id(), jobCrudStore.findActiveByBusinessKey(key).orElseThrow().getId());
    } finally {
      endCallerTransaction();
    }
  }

  private JobHandle commitHolderOnAnotherThread(String key, String idempotencyKey)
      throws Exception {
    ManagedExecutorService executor =
        InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
    return executor
        .submit(
            () -> {
              try {
                var builder = jobService.schedule(LATER, SimpleJob::execute).withBusinessKey(key);
                if (idempotencyKey != null) builder.withIdempotencyKey(idempotencyKey);
                return builder.submit();
              } finally {
                endCallerTransaction();
              }
            })
        .get(30, TimeUnit.SECONDS);
  }

  private static DataSource dataSource() throws NamingException {
    return InitialContext.doLookup("java:jboss/datasources/RatchetDS");
  }

  private static void awaitBlockedSession() throws NamingException {
    String query =
        switch (System.getProperty("ratchet.test.db.type")) {
          // JdbcContainerExtension grants the test user PROCESS, which INNODB_TRX needs.
          case "mysql" ->
              "SELECT COUNT(*) FROM information_schema.INNODB_TRX WHERE trx_state = 'LOCK WAIT'";
          case "postgresql" -> "SELECT COUNT(*) FROM pg_locks WHERE NOT granted";
          case "sqlserver" ->
              "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id <> 0";
          // The Oracle test user cannot read v$session; the 500 ms check stands alone there.
          case "oracle" -> null;
          default -> throw new AssertionError("Unsupported database for lock-wait observation");
        };
    if (query == null) {
      return;
    }
    DataSource dataSource = dataSource();
    JdbcLockWait.awaitBlockedSession(dataSource::getConnection, query, Duration.ofSeconds(30));
  }

  private record RaceOutcome(
      JobHandle ownWrite, JobHandle handle, DuplicateBusinessKeyException duplicate) {}

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
