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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;

class OracleTransientExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("01900000-0000-7000-8000-000000000001");

  @Test
  void archiveJobTranslatesTransientReadFailure() {
    OracleStoreContext ctx = throwingContext("getResultList");
    OracleJobRowMapper mapper = new OracleJobRowMapper();
    OracleTagOperations tags = new OracleTagOperations(ctx);
    OracleJobReadOperations reads = new OracleJobReadOperations(ctx, mapper, tags);
    OracleJobCrudOperations jobs = new OracleJobCrudOperations(reads, null, null, null, tags);
    OracleArchiveOperations archives = new OracleArchiveOperations(ctx, mapper, tags, jobs);
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);

    assertThrows(
        RatchetTransientStoreException.class, () -> archives.archiveJob(job, "ttl", "node-1"));
  }

  @Test
  void translateLeavesAnAlreadyTranslatedTransientUnwrapped() {
    // The EM proxy is never touched here; only dialectLabel()/constraintDetector() run.
    OracleStoreContext ctx = throwingContext("executeUpdate");
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
    OracleBatchOperations batches = new OracleBatchOperations(throwingContext("executeUpdate"));
    BatchEntity batch = new BatchEntity();
    batch.setId(JOB_ID);

    assertThrows(RatchetTransientStoreException.class, () -> batches.saveBatch(batch));
  }

  @Test
  void resetOrphanJobsTranslatesTransientWriteFailure() {
    OracleStoreContext ctx = throwingContext("executeUpdate");
    OracleJobDeleteOperations deletes =
        new OracleJobDeleteOperations(ctx, new OracleBusinessKeyReservations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> deletes.resetOrphanJobs(Duration.ofSeconds(30)));
  }

  @Test
  void findByIdTranslatesTransientReadFailure() {
    OracleStoreContext ctx = throwingContext("getResultList");
    OracleTagOperations tags = new OracleTagOperations(ctx);
    OracleJobReadOperations reads =
        new OracleJobReadOperations(ctx, new OracleJobRowMapper(), tags);

    assertThrows(RatchetTransientStoreException.class, () -> reads.findById(JOB_ID));
  }

  @Test
  void transitionToPausedTranslatesTransientWriteFailure() {
    OracleJobStatusTransitions transitions =
        new OracleJobStatusTransitions(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> transitions.transitionToPaused(JOB_ID, JobStatus.PENDING));
  }

  @Test
  void markFailedTerminalTranslatesTransientWriteFailure() {
    OracleStoreContext ctx = throwingContext("executeUpdate");
    OracleJobTerminalOperations terminals =
        new OracleJobTerminalOperations(
            ctx, new OracleBusinessKeyReservations(ctx), new OracleBatchOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> terminals.markJobFailedTerminal(JOB_ID, "boom", 1));
  }

  @Test
  void tryLockTranslatesTransientWriteFailure() {
    OracleNodeLockOperations locks = new OracleNodeLockOperations(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> locks.tryLock("lock", Duration.ofSeconds(30), "node-1"));
  }

  @Test
  void deliverSignalTranslatesTransientWriteFailure() {
    OracleSignalOperations signals = new OracleSignalOperations(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            signals.deliverSignalByKey(
                "signal", "{}", "json", "ACCEPTED", null, "node-1", Instant.EPOCH, "delivery"));
  }

  @Test
  void tagOperationsTranslateTransientWriteFailure() {
    OracleTagOperations tags = new OracleTagOperations(throwingContext("executeUpdate"));

    assertThrows(RatchetTransientStoreException.class, () -> tags.deleteTagsByJobId(JOB_ID));
  }

  @Test
  void resourcePermitReleaseTranslatesTransientWriteFailure() {
    OracleAuxiliaryOperations auxiliary =
        new OracleAuxiliaryOperations(throwingContext("executeUpdate"));

    assertThrows(
        RatchetTransientStoreException.class, () -> auxiliary.releasePermit("api", JOB_ID));
  }

  @Test
  void countByNativeTranslatesTransientReadFailure() {
    OracleJobCountOperations counts =
        new OracleJobCountOperations(throwingContext("getSingleResult"));

    assertThrows(RatchetTransientStoreException.class, counts::countActiveNodes);
  }

  private static OracleStoreContext throwingContext(String throwingQueryMethod) {
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
    return new OracleStoreContext(em, null);
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
