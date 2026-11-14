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

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;

class MysqlTransientExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("01900000-0000-7000-8000-000000000001");

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
    Object[] snapshot = {"PENDING", storedTime, 0, null, null, null, null, 7, null, null, 3};
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
                  int statement = statements.getAndIncrement();
                  String sql = (String) args[0];
                  if (statement == 1) {
                    assertTrue(sql.contains("WHERE job_id = ? AND status = ? AND version = ?"));
                  } else if (statement == 2) {
                    assertTrue(sql.contains("UPDATE scheduler_job"));
                    assertTrue(sql.contains("SET params = "));
                  } else {
                    assertEquals(0, statement);
                  }
                  return Proxy.newProxyInstance(
                      Query.class.getClassLoader(),
                      new Class<?>[] {Query.class},
                      (query, queryMethod, parameters) -> {
                        switch (queryMethod.getName()) {
                          case "setParameter":
                            if (statement == 1 && Integer.valueOf(4).equals(parameters[0])) {
                              assertEquals(7, parameters[1]);
                              versionBindings.incrementAndGet();
                            }
                            return query;
                          case "getResultList":
                            assertEquals(0, statement);
                            return Collections.singletonList(snapshot);
                          case "executeUpdate":
                            if (statement == 1) {
                              return hotUpdateCount;
                            }
                            assertEquals(2, statement);
                            coldUpdates.incrementAndGet();
                            return 1;
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
      assertEquals(2, statements.get());
      assertEquals(0, coldUpdates.get());
    } else {
      writes.save(job);
      assertEquals(3, statements.get());
      assertEquals(1, coldUpdates.get());
      assertEquals(4, job.getRowVersion());
    }
    assertEquals(1, versionBindings.get());
    assertEquals(7, job.getVersion());
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
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
