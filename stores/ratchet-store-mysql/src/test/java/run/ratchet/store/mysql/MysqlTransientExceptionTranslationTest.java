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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;

class MysqlTransientExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("01900000-0000-7000-8000-000000000001");

  @Test
  void stateOperationsTranslateTransientFailures() {
    RuntimeException failure = new RuntimeException(new SQLTransientConnectionException("lost"));
    var states = stateOperations(failure, false);
    assertThrows(
        RatchetTransientStoreException.class, () -> states.getState(UUID.randomUUID(), "test"));
    assertThrows(
        RatchetTransientStoreException.class,
        () -> states.updateState(UUID.randomUUID(), "test", "{}", 0, 7L));
  }

  @Test
  void stateOperationsPreservePermanentFailure() {
    RuntimeException failure = new RuntimeException(new SQLException("syntax error", "42000"));
    var states = stateOperations(failure, false);
    assertSame(
        failure,
        assertThrows(RuntimeException.class, () -> states.getState(UUID.randomUUID(), "test")));
    assertSame(
        failure,
        assertThrows(
            RuntimeException.class,
            () -> states.updateState(UUID.randomUUID(), "test", "{}", 0, 7L)));
  }

  @Test
  void plainStateInitPreservesDuplicateMessageAndCause() {
    RuntimeException duplicate =
        new RuntimeException(new SQLException("Duplicate entry", "23000", 1062));
    var thrown =
        assertThrows(
            IllegalStateException.class,
            () -> stateOperations(duplicate, true).initState(UUID.randomUUID(), "test", "{}"));
    assertTrue(thrown.getMessage().startsWith("extension state already initialized"));
    assertSame(duplicate, thrown.getCause());
  }

  @Test
  void fencedStateInitPreservesDuplicateDetection() {
    RuntimeException duplicate =
        new RuntimeException(new SQLException("Duplicate entry", "23000", 1062));
    assertEquals(
        StateWriteResult.VERSION_CONFLICT,
        stateOperations(duplicate, true).initState(UUID.randomUUID(), "test", "{}", 7L));
  }

  private MysqlExtensionOperations stateOperations(RuntimeException failure, boolean duplicate) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if (!"createNativeQuery".equals(method.getName()))
                    throw new AssertionError(method.getName());
                  String sql = (String) args[0];
                  return Proxy.newProxyInstance(
                      Query.class.getClassLoader(),
                      new Class<?>[] {Query.class},
                      (query, operation, parameters) -> {
                        switch (operation.getName()) {
                          case "setParameter":
                            return query;
                          case "getResultList":
                            if (!duplicate && sql.contains("SELECT state, version")) throw failure;
                            if (sql.contains("SELECT claim_seq, status"))
                              return List.<Object[]>of(new Object[] {7L, "RUNNING"});
                            return List.of(0);
                          case "executeUpdate":
                            throw failure;
                          default:
                            throw new AssertionError(operation.getName());
                        }
                      });
                });
    return new MysqlExtensionOperations(new MysqlStoreContext(em, null));
  }

  @Test
  void scheduledTimeSaveRejectsConcurrentModificationBeforeColdUpdate() {
    assertScheduledTimeSave(0);
  }

  @Test
  void scheduledTimeSaveAlsoUpdatesColdRow() {
    assertScheduledTimeSave(1);
  }

  @Test
  void scheduledTimeSaveKeepsVersionsWhenColdUpdateFails() {
    assertScheduledTimeSave(1, 0);
  }

  private static void assertScheduledTimeSave(int hotUpdateCount) {
    assertScheduledTimeSave(hotUpdateCount, 1);
  }

  private static void assertScheduledTimeSave(int hotUpdateCount, int coldUpdateCount) {
    Instant storedTime = Instant.parse("2026-11-14T00:00:00Z");
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setStatus(JobStatus.PENDING);
    job.setVersion(7);
    job.setRowVersion(3);
    job.setScheduledTime(storedTime.plusSeconds(60));
    job.setResourceName("changed-resource");
    Object[] snapshot = {"PENDING", storedTime, 0, null, null, null, null, 7, null, null, 3};
    AtomicInteger statements = new AtomicInteger();
    AtomicInteger coldUpdates = new AtomicInteger();
    AtomicInteger versionBindings = new AtomicInteger();
    AtomicInteger resourceBindings = new AtomicInteger();
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if (!"createNativeQuery".equals(method.getName())) {
                    throw new UnsupportedOperationException(method.getName());
                  }
                  int statement = statements.getAndIncrement();
                  String sql = (String) args[0];
                  if (statement == 0) {
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
                            if (statement == 3 && Integer.valueOf(6).equals(parameters[0])) {
                              assertEquals("changed-resource", parameters[1]);
                              resourceBindings.incrementAndGet();
                            }
                            return query;
                          case "getResultList":
                            if (statement == 0) {
                              return List.of(JOB_ID);
                            }
                            assertEquals(1, statement);
                            return Collections.singletonList(snapshot);
                          case "executeUpdate":
                            if (statement == 2) {
                              return hotUpdateCount;
                            }
                            assertEquals(3, statement);
                            coldUpdates.incrementAndGet();
                            return coldUpdateCount;
                          default:
                            throw new UnsupportedOperationException(queryMethod.getName());
                        }
                      });
                });
    var ctx = new MysqlStoreContext(em, null);
    var writes =
        new MysqlJobWriteOperations(
            ctx, new MysqlBusinessKeyReservations(ctx), new MysqlTagOperations(ctx));
    if (hotUpdateCount == 0) {
      var failure = assertThrows(RatchetOptimisticLockException.class, () -> writes.save(job));
      assertEquals("Concurrent modification on job " + JOB_ID, failure.getMessage());
      assertEquals(3, statements.get());
      assertEquals(0, coldUpdates.get());
    } else if (coldUpdateCount == 0) {
      var failure = assertThrows(RatchetOptimisticLockException.class, () -> writes.save(job));
      assertEquals("Concurrent modification on job " + JOB_ID, failure.getMessage());
      assertEquals(4, statements.get());
      assertEquals(1, coldUpdates.get());
      assertEquals(3, job.getRowVersion());
    } else {
      writes.save(job);
      assertEquals(4, statements.get());
      assertEquals(1, coldUpdates.get());
      assertEquals(1, resourceBindings.get());
      assertEquals(4, job.getRowVersion());
    }
    assertEquals(1, versionBindings.get());
    assertEquals(hotUpdateCount == 0 || coldUpdateCount == 0 ? 7 : 8, job.getVersion());
  }

  @Test
  void staleSaveLeavesTheWaiterRowAlone() {
    Instant storedTime = Instant.parse("2026-11-14T00:00:00Z");
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setStatus(JobStatus.PENDING);
    job.setVersion(7);
    job.setRowVersion(3);
    job.setScheduledTime(storedTime.plusSeconds(60));
    job.setResourceName("changed-resource");
    Object[] snapshot = {
      "PENDING", storedTime, 0, null, null, null, null, 8, null, "stored-resource", 3
    };
    AtomicInteger reads = new AtomicInteger();
    AtomicInteger waiterStatements = new AtomicInteger();
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
                  if (sql.contains("scheduler_resource_waiter")) {
                    waiterStatements.incrementAndGet();
                  }
                  return Proxy.newProxyInstance(
                      Query.class.getClassLoader(),
                      new Class<?>[] {Query.class},
                      (query, queryMethod, parameters) -> {
                        return switch (queryMethod.getName()) {
                          case "setParameter" -> query;
                          case "getResultList" ->
                              reads.getAndIncrement() == 0
                                  ? List.of(JOB_ID)
                                  : Collections.singletonList(snapshot);
                          case "executeUpdate" ->
                              // A stale save must fail on the version check, before it writes.
                              throw new AssertionError("Unexpected update: " + sql);
                          default -> throw new UnsupportedOperationException(queryMethod.getName());
                        };
                      });
                });
    var writes = writeOperations(em);

    assertThrows(RatchetOptimisticLockException.class, () -> writes.save(job));
    assertEquals(0, waiterStatements.get());
  }

  @Test
  void saveTranslatesSnapshotDeadlock() {
    var writes =
        writeOperations(entityManager(queryReturning(List.of(JOB_ID)), queryThrowingOnResults()));

    assertThrows(RatchetTransientStoreException.class, () -> writes.save(existingJob()));
  }

  @Test
  void saveTranslatesColdUpdateDeadlock() {
    Object[] snapshot = {"PENDING", null, 0, null, null, null, null, 0, null, null, 0};
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
    // Cold and hot INSERTs are prepared before the idempotency reservation writes first.
    var writes =
        writeOperations(
            entityManager(
                queryReturningUpdate(1), queryReturningUpdate(1), queryThrowingOnExecute()));
    JobEntity job = existingJob();
    job.setIdempotencyKey("key");

    assertThrows(RatchetTransientStoreException.class, () -> writes.saveInsert(job));
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

  private static MysqlJobWriteOperations writeOperations(EntityManager em) {
    var ctx = new MysqlStoreContext(em, null);
    return new MysqlJobWriteOperations(
        ctx, new MysqlBusinessKeyReservations(ctx), new MysqlTagOperations(ctx));
  }

  @Test
  void archiveJobTranslatesTransientReadFailure() {
    MysqlStoreContext ctx = throwingContext("getResultList");
    MysqlJobRowMapper mapper = new MysqlJobRowMapper();
    MysqlTagOperations tags = new MysqlTagOperations(ctx);
    MysqlJobReadOperations reads = new MysqlJobReadOperations(ctx, mapper, tags);
    MysqlJobCrudOperations jobs = new MysqlJobCrudOperations(reads, null, null, null, tags);
    MysqlArchiveOperations archives = new MysqlArchiveOperations(ctx, mapper, tags, jobs);
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);

    assertThrows(
        RatchetTransientStoreException.class, () -> archives.archiveJob(job, "ttl", "node-1"));
  }

  @Test
  void translateLeavesAnAlreadyTranslatedTransientUnwrapped() {
    // The EM proxy is never touched here; only dialectLabel()/constraintDetector() run.
    MysqlStoreContext ctx = throwingContext("executeUpdate");
    RatchetTransientStoreException alreadyTranslated =
        new RatchetTransientStoreException(
            "inner operation", new SQLTransientException("connection lost"));

    RuntimeException result =
        ctx.translateTransientStoreException("outer operation", alreadyTranslated);

    // A nested operation already translated this; re-translating must not double-wrap. The
    // detector would otherwise walk the cause chain, re-detect the transient, and re-wrap.
    assertSame(alreadyTranslated, result);
  }

  @Test
  void saveBatchTranslatesTransientWriteFailure() {
    MysqlBatchOperations batches = new MysqlBatchOperations(throwingContext("executeUpdate"));
    BatchEntity batch = new BatchEntity();
    batch.setId(JOB_ID);

    assertThrows(RatchetTransientStoreException.class, () -> batches.saveBatch(batch));
  }

  @Test
  void resetOrphanJobsTranslatesTransientFailure() {
    // The scan's first statement is the non-locking candidate read.
    MysqlStoreContext ctx = throwingContext("getResultList");
    MysqlJobDeleteOperations deletes =
        new MysqlJobDeleteOperations(ctx, new MysqlBusinessKeyReservations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> deletes.resetOrphanJobsBefore(Instant.now().minusSeconds(30), 3, 500).reset());
  }

  @Test
  void findByIdTranslatesTransientReadFailure() {
    MysqlStoreContext ctx = throwingContext("getResultList");
    MysqlTagOperations tags = new MysqlTagOperations(ctx);
    MysqlJobReadOperations reads = new MysqlJobReadOperations(ctx, new MysqlJobRowMapper(), tags);

    assertThrows(RatchetTransientStoreException.class, () -> reads.findById(JOB_ID));
  }

  @Test
  void transitionToPausedTranslatesTransientWriteFailure() {
    MysqlJobStatusTransitions transitions =
        new MysqlJobStatusTransitions(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> transitions.transitionToPaused(JOB_ID, JobStatus.PENDING));
  }

  @Test
  void markFailedTerminalTranslatesTransientWriteFailure() {
    MysqlStoreContext ctx = throwingContext("executeUpdate");
    MysqlJobTerminalOperations terminals =
        new MysqlJobTerminalOperations(
            ctx, new MysqlBusinessKeyReservations(ctx), new MysqlBatchOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> terminals.markJobFailedTerminal(JOB_ID, "boom", 1, null));
  }

  @Test
  void tryLockTranslatesTransientWriteFailure() {
    MysqlNodeLockOperations locks = new MysqlNodeLockOperations(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> locks.tryLock("lock", Duration.ofSeconds(30), "node-1"));
  }

  @Test
  void deliverSignalTranslatesTransientWriteFailure() {
    MysqlSignalOperations signals = new MysqlSignalOperations(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            signals.deliverSignalByKey(
                "signal", "{}", "json", "ACCEPTED", null, "node-1", Instant.EPOCH, "delivery"));
  }

  @Test
  void tagOperationsTranslateTransientWriteFailure() {
    MysqlTagOperations tags = new MysqlTagOperations(throwingContext("executeUpdate"));

    assertThrows(RatchetTransientStoreException.class, () -> tags.deleteTagsByJobId(JOB_ID));
  }

  @Test
  void resourcePermitReleaseTranslatesTransientWriteFailure() {
    MysqlAuxiliaryOperations auxiliary =
        new MysqlAuxiliaryOperations(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class, () -> auxiliary.releasePermit("api", JOB_ID, 0L));
  }

  @Test
  void countByNativeTranslatesTransientReadFailure() {
    MysqlJobCountOperations counts =
        new MysqlJobCountOperations(throwingContext("getSingleResult"));

    assertThrows(RatchetTransientStoreException.class, counts::countActiveNodes);
  }

  @Test
  void permitInsertFlushTranslatesTransientFailureBeforeCommit() {
    EntityManager em = mock(EntityManager.class);
    Query limit = mock(Query.class);
    Query permits = mock(Query.class);
    Query claim = mock(Query.class);
    Query waiters = mock(Query.class);
    when(em.createNativeQuery(contains("SELECT max_concurrent"))).thenReturn(limit);
    when(em.createNativeQuery(contains("SELECT id FROM scheduler_resource_permit")))
        .thenReturn(permits);
    when(em.createNativeQuery(contains("SELECT q.status, q.claim_seq"))).thenReturn(claim);
    when(em.createNativeQuery(contains("SELECT w.job_id FROM scheduler_resource_waiter")))
        .thenReturn(waiters);
    for (Query query : List.of(limit, permits, claim, waiters)) {
      when(query.setParameter(anyInt(), any())).thenReturn(query);
    }
    when(limit.getResultList()).thenReturn(List.<Object[]>of(new Object[] {2, 100}));
    when(claim.getResultList())
        .thenReturn(
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 0, null, null, null, null, "node"}));
    when(permits.getResultList()).thenReturn(List.of());
    when(waiters.getResultList()).thenReturn(List.of());
    doThrow(new RuntimeException("flush", new SQLTransientException("deadlock"))).when(em).flush();
    var auxiliary = new MysqlAuxiliaryOperations(new MysqlStoreContext(em, null));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> auxiliary.tryAcquirePermit("api", JOB_ID, 7L, "node"));
    var order = inOrder(em);
    order.verify(em).persist(any());
    order.verify(em).flush();
  }

  @Test
  void permitCleanupTranslatesTransientFailureAtEveryStatement() {
    for (String failingSql :
        List.of(
            "SELECT DISTINCT",
            "WHERE resource_name IN",
            "FOR UPDATE",
            "LEFT JOIN scheduler_job_queue",
            "DELETE FROM scheduler_resource_permit WHERE id IN")) {
      EntityManager em = mock(EntityManager.class);
      Query discovery = mock(Query.class);
      Query keys = mock(Query.class);
      Query lock = mock(Query.class);
      Query deletable = mock(Query.class);
      Query delete = mock(Query.class);
      when(em.createNativeQuery(any(String.class)))
          .thenAnswer(
              invocation -> {
                String sql = invocation.getArgument(0);
                if (sql.contains(failingSql)) {
                  throw new RuntimeException("jpa", new SQLTransientException("deadlock"));
                }
                if (sql.contains("SELECT p.id")) return deletable;
                if (sql.contains("SELECT DISTINCT")) return discovery;
                if (sql.contains("WHERE resource_name IN")) return keys;
                if (sql.contains("FOR UPDATE")) return lock;
                return delete;
              });
      for (Query query : List.of(discovery, keys, lock, deletable, delete)) {
        lenient().when(query.setParameter(anyInt(), any())).thenReturn(query);
      }
      lenient().when(discovery.getResultList()).thenReturn(List.of("api"));
      lenient().when(keys.getResultList()).thenReturn(List.of("api"));
      lenient().when(lock.getResultList()).thenReturn(List.of("api"));
      lenient().when(deletable.getResultList()).thenReturn(List.of(new byte[16]));
      var auxiliary = new MysqlAuxiliaryOperations(new MysqlStoreContext(em, null));
      assertThrows(
          RatchetTransientStoreException.class,
          () -> auxiliary.cleanupOrphanedPermits(List.of("stale")),
          failingSql);
      assertThrows(
          RatchetTransientStoreException.class,
          () -> auxiliary.cleanupPermitsOfMissingNodes(Instant.EPOCH),
          failingSql);
    }
  }

  private static MysqlStoreContext throwingContext(String throwingQueryMethod) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName())) {
                    return queryThrowingOn(throwingQueryMethod);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    return new MysqlStoreContext(em, null);
  }

  private static Query queryThrowingOn(String throwingMethod) {
    RuntimeException failure =
        new RuntimeException("jpa", new SQLTransientException("connection lost"));
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if (method.getName().equals(throwingMethod)) {
                throw failure;
              }
              return switch (method.getName()) {
                case "setParameter", "setFirstResult", "setMaxResults" -> proxy;
                // A queue-row lock finds the expected claim, so the write itself runs.
                case "getResultList" -> List.<Object[]>of(new Object[] {"node", 1L});
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
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

  private static Query query(
      List<?> rows, int updated, RuntimeException executeFailure, RuntimeException resultsFailure) {
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
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  private static RuntimeException deadlock() {
    return new PersistenceException("deadlock", new SQLException("deadlock", "40001", 1213));
  }
}
