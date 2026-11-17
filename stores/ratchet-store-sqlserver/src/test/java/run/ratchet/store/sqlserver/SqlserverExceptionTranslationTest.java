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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class SqlserverExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000031");

  @Test
  void scheduledTimeSaveRejectsConcurrentModificationBeforeColdUpdate() {
    assertScheduledTimeSave(0);
  }

  @Test
  void scheduledTimeSaveAlsoUpdatesColdRow() {
    assertScheduledTimeSave(1);
  }

  private static void assertScheduledTimeSave(int hotUpdateCount) {
    Instant storedTime = Instant.parse("2026-11-14T00:00:00Z");
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setStatus(JobStatus.PENDING);
    job.setVersion(7);
    job.setRowVersion(3);
    job.setScheduledTime(storedTime.plusSeconds(60));
    job.setResourceName("changed-resource");
    Object[] snapshot = {"PENDING", storedTime, 0, null, null, null, null, 7, null, 3};
    AtomicInteger statements = new AtomicInteger();
    AtomicInteger coldUpdates = new AtomicInteger();
    AtomicInteger versionBindings = new AtomicInteger();
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if (!"createNativeQuery".equals(method.getName())) {
                    throw new UnsupportedOperationException(method.getName());
                  }
                  String sql = (String) args[0];
                  boolean waiterCleanup = sql.contains("DELETE FROM scheduler_resource_waiter");
                  int statement = waiterCleanup ? -1 : statements.getAndIncrement();
                  if (waiterCleanup) {
                    assertTrue(sql.contains("WHERE job_id = ?"));
                  } else if (statement == 0) {
                    assertTrue(sql.contains("SELECT job_id FROM scheduler_job_queue"));
                  } else if (statement == 2) {
                    assertTrue(sql.contains("WHERE job_id = ? AND status = ? AND version = ?"));
                  } else if (statement == 3) {
                    assertTrue(sql.contains("UPDATE scheduler_job"));
                    assertTrue(sql.contains("SET params = "));
                  } else {
                    assertEquals(1, statement);
                  }
                  return Proxy.newProxyInstance(
                      Query.class.getClassLoader(),
                      new Class<?>[] {Query.class},
                      (query, queryMethod, parameters) -> {
                        switch (queryMethod.getName()) {
                          case "setParameter":
                            if (statement == 2 && Integer.valueOf(4).equals(parameters[0])) {
                              assertEquals(7, parameters[1]);
                              versionBindings.incrementAndGet();
                            }
                            return query;
                          case "getResultList":
                            if (statement == 0) {
                              return List.of(JOB_ID);
                            }
                            assertEquals(1, statement);
                            return Collections.singletonList(snapshot);
                          case "executeUpdate":
                            if (waiterCleanup) {
                              return 1;
                            }
                            if (statement == 2) {
                              return hotUpdateCount;
                            }
                            assertEquals(3, statement);
                            coldUpdates.incrementAndGet();
                            return 1;
                          default:
                            throw new UnsupportedOperationException(queryMethod.getName());
                        }
                      });
                });
    var ctx = new SqlserverStoreContext(em);
    var writes =
        new SqlserverJobWriteOperations(
            ctx, new SqlserverBusinessKeyReservations(ctx), new SqlserverTagOperations(ctx));
    if (hotUpdateCount == 0) {
      var failure = assertThrows(RatchetOptimisticLockException.class, () -> writes.save(job));
      assertEquals("Concurrent modification on job " + JOB_ID, failure.getMessage());
      assertEquals(3, statements.get());
      assertEquals(0, coldUpdates.get());
    } else {
      writes.save(job);
      assertEquals(4, statements.get());
      assertEquals(1, coldUpdates.get());
      assertEquals(4, job.getRowVersion());
    }
    assertEquals(1, versionBindings.get());
    assertEquals(hotUpdateCount == 0 ? 7 : 8, job.getVersion());
  }

  @Test
  void saveTranslatesSnapshotDeadlock() {
    var writes =
        writeOperations(entityManager(queryReturning(List.of(JOB_ID)), queryThrowingOnResults()));

    assertThrows(RatchetTransientStoreException.class, () -> writes.save(existingJob()));
  }

  @Test
  void saveTranslatesColdUpdateDeadlock() {
    Object[] snapshot = {"PENDING", null, 0, null, null, null, null, 0, null, 0};
    var writes =
        writeOperations(
            entityManager(
                queryReturning(List.of(JOB_ID)),
                queryReturning(Collections.singletonList(snapshot)),
                queryThrowingOnExecute()));

    assertThrows(RatchetTransientStoreException.class, () -> writes.save(existingJob()));
  }

  @Test
  void saveInsertTranslatesFirstWriteDeadlock() {
    // Cold and hot INSERTs are prepared before the idempotency reservation writes first. The
    // reservation reads its outcome as a single result.
    var writes =
        writeOperations(
            entityManager(
                queryReturningUpdate(1), queryReturningUpdate(1), queryThrowingOnSingleResult()));

    assertThrows(RatchetTransientStoreException.class, () -> writes.saveInsert(existingJob()));
  }

  @Test
  void bulkInsertTranslatesFirstWriteDeadlock() {
    var writes = writeOperations(entityManager(queryThrowingOnExecute()));

    assertThrows(
        RatchetTransientStoreException.class, () -> writes.bulkInsert(List.of(existingJob())));
  }

  @Test
  void saveMissingJobLeavesIllegalStateExceptionUnchanged() {
    var writes =
        writeOperations(entityManager(queryReturning(List.of()), queryReturning(List.of())));

    assertThrows(IllegalStateException.class, () -> writes.save(existingJob()));
  }

  private static JobEntity existingJob() {
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setJobType(JobExecutionType.SINGLE);
    job.setVersion(0);
    job.setRowVersion(0);
    return job;
  }

  private static SqlserverJobWriteOperations writeOperations(EntityManager em) {
    var ctx = new SqlserverStoreContext(em);
    return new SqlserverJobWriteOperations(
        ctx, new SqlserverBusinessKeyReservations(ctx), new SqlserverTagOperations(ctx));
  }

  @Test
  void archiveJobTranslatesInsertDeadlock() {
    EntityManager em =
        entityManager(
            queryReturning(Collections.singletonList(terminalRow())),
            queryReturning(List.of()),
            // archive copy of extension data: properties + extension-state reads
            queryReturning(List.of()),
            queryReturning(List.of()),
            queryThrowingOnExecute());
    var ctx = new SqlserverStoreContext(em);
    var tags = new SqlserverTagOperations(ctx);
    var reads = new SqlserverJobReadOperations(ctx, tags);
    var deletes = new SqlserverJobDeleteOperations(ctx, new SqlserverBusinessKeyReservations(ctx));
    var archives = new SqlserverArchiveOperations(ctx, reads, deletes);
    JobEntity input = new JobEntity();
    input.setId(JOB_ID);

    assertThrows(
        RatchetTransientStoreException.class,
        () -> archives.archiveJob(input, "retention", "test"));
  }

  @Test
  void findJobsForArchivingTranslatesDeadlock() {
    var archives = archiveOperations(entityManager(queryThrowingOnResults()));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> archives.findJobsForArchiving(Instant.now(), 10));
  }

  @Test
  void findJobsForArchivingDoesNotRewrapInnerTagFailure() {
    var archives =
        archiveOperations(
            entityManager(
                queryReturning(Collections.singletonList(terminalRow())),
                queryThrowingOnResults()));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> archives.findJobsForArchiving(Instant.now(), 10));
    // The guard rethrows the inner transient as-is; it is not re-translated to the outer op label.
    assertTrue(thrown.getMessage().contains("hydrate job tags batch"));
  }

  @Test
  void findArchivedJobsTranslatesDeadlock() {
    var archives = archiveOperations(entityManager(queryThrowingOnResults()));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> archives.findArchivedJobs("Job", null, null, null, 10));
  }

  @Test
  void purgeArchivedJobsTranslatesDeadlock() {
    var archives = archiveOperations(entityManager(queryThrowingOnExecute()));

    assertThrows(
        RatchetTransientStoreException.class, () -> archives.purgeArchivedJobs(Instant.now()));
  }

  private static SqlserverArchiveOperations archiveOperations(EntityManager em) {
    var ctx = new SqlserverStoreContext(em);
    var tags = new SqlserverTagOperations(ctx);
    var reads = new SqlserverJobReadOperations(ctx, tags);
    var deletes = new SqlserverJobDeleteOperations(ctx, new SqlserverBusinessKeyReservations(ctx));
    return new SqlserverArchiveOperations(ctx, reads, deletes);
  }

  @Test
  void businessKeyReservationsTranslateDeadlock() {
    var reservations =
        new SqlserverBusinessKeyReservations(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            reservations.insertReservation(
                "key", JOB_ID, SqlserverBusinessKeyReservations.OWNER_TABLE_QUEUE));
  }

  @Test
  void deleteOperationsTranslateDeadlock() {
    // The scan's first statement is the non-locking candidate read.
    var deletes =
        new SqlserverJobDeleteOperations(
            new SqlserverStoreContext(entityManager(queryThrowingOnResults())),
            new SqlserverBusinessKeyReservations(
                new SqlserverStoreContext(entityManager(queryReturningUpdate(1)))));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> deletes.resetOrphanJobsForNode("node", 3, 500).reset());
  }

  @Test
  void readOperationsTranslateDeadlock() {
    var ctx = new SqlserverStoreContext(entityManager(queryThrowingOnResults()));
    var reads = new SqlserverJobReadOperations(ctx, new SqlserverTagOperations(ctx));

    assertThrows(RatchetTransientStoreException.class, () -> reads.findById(JOB_ID));
  }

  @Test
  void queryOperationsTranslateSearchDeadlock() {
    var ctx = new SqlserverStoreContext(entityManager(queryThrowingOnResults()));
    var queries = new SqlserverJobQueryOperations(ctx, new SqlserverTagOperations(ctx));

    assertThrows(RatchetTransientStoreException.class, () -> queries.searchJobs(null, 10, 0));
  }

  @Test
  void queryOperationsTranslateCountDeadlock() {
    var ctx = new SqlserverStoreContext(entityManager(queryThrowingOnSingleResult()));
    var queries = new SqlserverJobQueryOperations(ctx, new SqlserverTagOperations(ctx));

    assertThrows(RatchetTransientStoreException.class, () -> queries.countJobs(null));
  }

  @Test
  void statusTransitionsTranslateDeadlock() {
    var transitions =
        new SqlserverJobStatusTransitions(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class, () -> transitions.tryPickUpJob(JOB_ID, "node"));
  }

  @Test
  void terminalOperationsTranslateDeadlock() {
    var ctx =
        new SqlserverStoreContext(
            entityManager(queryReturning(List.of()), queryThrowingOnExecute()));
    var terminals =
        new SqlserverJobTerminalOperations(
            ctx, new SqlserverBusinessKeyReservations(ctx), new SqlserverBatchOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> terminals.scheduleJobRetry(JOB_ID, "boom", Instant.now(), 1, null));
  }

  @Test
  void tagOperationsTranslateDeadlock() {
    var tags =
        new SqlserverTagOperations(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(RatchetTransientStoreException.class, () -> tags.deleteTagsByJobId(JOB_ID));
  }

  @Test
  void nodeLockOperationsTranslateDeadlock() {
    var locks =
        new SqlserverNodeLockOperations(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> locks.tryLock("scheduler", Duration.ofSeconds(30), "node-1"));
  }

  @Test
  void signalOperationsTranslateDeadlock() {
    var signals =
        new SqlserverSignalOperations(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            signals.deliverSignalByKey(
                "approval", "{}", "json", "APPROVED", null, "node-1", Instant.EPOCH, "delivery-1"));
  }

  @Test
  void resourcePermitReleaseTranslatesDeadlock() {
    var auxiliary =
        new SqlserverAuxiliaryOperations(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class, () -> auxiliary.releasePermit("api", JOB_ID, 0L));
  }

  private static EntityManager entityManager(Query... queries) {
    AtomicInteger nextQuery = new AtomicInteger();
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return queries[nextQuery.getAndIncrement()];
              }
              if ("contains".equals(method.getName())) {
                return false;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query queryReturning(List<?> rows) {
    return query(rows, 1, null, null);
  }

  private static Query queryReturningUpdate(int updated) {
    return query(List.of(), updated, null, null);
  }

  private static Query queryThrowingOnExecute() {
    return query(List.of(), 0, deadlock(), null);
  }

  private static Query queryThrowingOnResults() {
    return query(List.of(), 0, null, deadlock());
  }

  private static Query queryThrowingOnSingleResult() {
    return query(List.of(), 0, null, null, deadlock());
  }

  private static Query query(
      List<?> rows, int updated, RuntimeException executeFailure, RuntimeException resultsFailure) {
    return query(rows, updated, executeFailure, resultsFailure, null);
  }

  private static Query query(
      List<?> rows,
      int updated,
      RuntimeException executeFailure,
      RuntimeException resultsFailure,
      RuntimeException singleResultFailure) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter", "setFirstResult", "setMaxResults" -> proxy;
                case "executeUpdate" -> {
                  if (executeFailure != null) {
                    throw executeFailure;
                  }
                  yield updated;
                }
                case "getResultList" -> {
                  if (resultsFailure != null) {
                    throw resultsFailure;
                  }
                  yield rows;
                }
                case "getSingleResult" -> {
                  if (singleResultFailure != null) {
                    throw singleResultFailure;
                  }
                  yield rows.isEmpty() ? 0 : rows.get(0);
                }
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  private static RuntimeException deadlock() {
    // SQL Server deadlock victim: vendor error 1205, SQLState 40001.
    return new PersistenceException(
        "deadlock", new SQLException("Transaction was deadlocked", "40001", 1205));
  }

  private static Object[] terminalRow() {
    Object[] row = new Object[SqlserverJobRowMapper.HYDRATION_COL_COUNT];
    row[53] = 0L;
    Instant now = Instant.parse("2026-05-12T14:30:00Z");
    row[0] = JOB_ID;
    row[1] = JobExecutionType.SINGLE.name();
    row[2] = JobPriority.NORMAL.persistedCode();
    row[3] = 3;
    row[4] = BackoffPolicy.NONE.name();
    row[5] = 0;
    row[6] = 60;
    row[11] = "example.Job"; // target_class
    row[12] = "run"; // method_name
    row[20] = now; // created_at
    row[22] = JobStatus.SUCCEEDED.name(); // terminal_status
    row[24] = 1; // total_attempts
    row[25] = now; // terminated_at
    row[26] = now; // execution_start_time
    row[27] = now; // execution_end_time
    return row;
  }
}
