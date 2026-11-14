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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.util.List;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.JobHandle;
import run.ratchet.api.KeyRange;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.tck.store.JdbcLockWait;
import run.ratchet.testsuite.app.PartitionJobs;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Verifies idempotency races preserve the caller's JTA transaction and its own writes. */
@EnabledIfSystemProperty(
    named = "ratchet.test.db.type",
    matches = "mysql|postgresql|oracle|sqlserver")
class IdempotencyKeyCallerTransactionIT extends BaseRatchetIT {

  private static final Duration LATER = Duration.ofHours(1);

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private UserTransaction utx;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class, PartitionJobs.class)
        .addClasses(JdbcLockWait.class, JdbcLockWait.ConnectionFactory.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  void endCallerTransaction() throws Exception {
    if (utx.getStatus() != Status.STATUS_NO_TRANSACTION) {
      utx.rollback();
    }
  }

  @Test
  @EnabledIf("onWildFly")
  void concurrentWinnerCommits_returnsOriginalAndCommitsCallerWrite() throws Exception {
    race(false);
  }

  @Test
  @EnabledIf("onWildFly")
  void concurrentWinnerRollsBack_callerCreatesAndCommits() throws Exception {
    race(true);
  }

  @Test
  @EnabledIf("onWildFly")
  void partitionedLoser_returnsOriginalAndCommitsCallerWrite() throws Exception {
    race(false, true);
  }

  @Test
  @EnabledIf("onWildFly")
  void repeatedPartitionKeyInCallerTransaction_returnsOriginalAndCommits() throws Exception {
    String partitionKey = UUID.randomUUID().toString();
    JobHandle first;
    try {
      utx.begin();
      first =
          jobService
              .enqueueBatch("caller partition")
              .forEachPartition(partitionKey, List.of(new KeyRange(0, 10)), PartitionJobs::process)
              .submit();
      JobHandle second =
          assertTimeout(
              Duration.ofSeconds(5),
              () ->
                  jobService
                      .enqueueBatch("caller partition")
                      .forEachPartition(
                          partitionKey, List.of(new KeyRange(0, 10)), PartitionJobs::process)
                      .submit());
      assertEquals(first.id(), second.id());
      assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
      utx.commit();
    } finally {
      endCallerTransaction();
    }
    assertTrue(jobCrudStore.findById(first.id()).isPresent());
  }

  static boolean onWildFly() {
    return System.getProperty("testsuite.profile", "").startsWith("wildfly")
        || System.getProperty("jboss.home.dir") != null;
  }

  private void race(boolean rollbackWinner) throws Exception {
    race(rollbackWinner, false);
  }

  private void race(boolean rollbackWinner, boolean partitionedLoser) throws Exception {
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
                  var builder = jobService.schedule(LATER, SimpleJob::execute);
                  builder.withIdempotencyKey(idempotencyKey);
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
                  attempting.countDown();
                  JobHandle handle;
                  if (partitionedLoser) {
                    handle =
                        jobService
                            .enqueueBatch("idempotency race")
                            .forEachPartition(
                                UUID.randomUUID().toString(),
                                List.of(new KeyRange(0, 10)),
                                PartitionJobs::process)
                            .withIdempotencyKey(idempotencyKey)
                            .submit();
                  } else {
                    var builder = jobService.schedule(LATER, SimpleJob::execute);
                    builder.withIdempotencyKey(idempotencyKey);
                    handle = builder.submit();
                  }
                  assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
                  // PostgreSQL would raise 25P02 here if the idempotency insert aborted the tx.
                  try (var connection = dataSource().getConnection();
                      var statement = connection.createStatement();
                      var rows =
                          statement.executeQuery(
                              "SELECT 1"
                                  + ("oracle".equals(System.getProperty("ratchet.test.db.type"))
                                      ? " FROM dual"
                                      : ""))) {
                    assertTrue(rows.next());
                    assertEquals(1, rows.getInt(1));
                  }
                  utx.commit();
                  return new RaceOutcome(ownWrite, handle);
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
      assertEquals(1, jobsWithIdempotencyKey(idempotencyKey));
      if (rollbackWinner) {
        assertTrue(jobCrudStore.findById(winningHandle.id()).isEmpty());
        assertEquals(
            result.handle().id(),
            jobCrudStore.findOriginalJobIdByIdempotencyKey(idempotencyKey).orElseThrow());
      } else {
        assertEquals(winningHandle.id(), result.handle().id());
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
    assertTrue(
        JdbcLockWait.awaitBlockedSession(dataSource::getConnection, query, Duration.ofSeconds(30)),
        "Loser did not block on the key");
  }

  private record RaceOutcome(JobHandle ownWrite, JobHandle handle) {}

  private int jobsWithIdempotencyKey(String key) throws Exception {
    try (var connection = dataSource().getConnection();
        var statement =
            connection.prepareStatement(
                "SELECT COUNT(*) FROM scheduler_job WHERE idempotency_key = ?")) {
      statement.setString(1, key);
      try (var rows = statement.executeQuery()) {
        assertTrue(rows.next());
        return rows.getInt(1);
      }
    }
  }
}
