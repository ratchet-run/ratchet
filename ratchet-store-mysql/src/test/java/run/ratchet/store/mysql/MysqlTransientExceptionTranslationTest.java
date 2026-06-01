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

class MysqlTransientExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("01900000-0000-7000-8000-000000000001");

  @Test
  void archiveJobTranslatesTransientReadFailure() {
    MysqlStoreContext ctx = throwingContext("getResultList");
    MysqlJobRowMapper mapper = new MysqlJobRowMapper();
    MysqlTagOperations tags = new MysqlTagOperations(ctx);
    MysqlJobReadOperations reads = new MysqlJobReadOperations(ctx, mapper, tags);
    MysqlJobCrudOperations jobs = new MysqlJobCrudOperations(reads, null, null, null);
    MysqlArchiveOperations archives = new MysqlArchiveOperations(ctx, mapper, tags, jobs);
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);

    assertThrows(
        RatchetTransientStoreException.class, () -> archives.archiveJob(job, "ttl", "node-1"));
  }

  @Test
  void saveBatchTranslatesTransientWriteFailure() {
    MysqlBatchOperations batches = new MysqlBatchOperations(throwingContext("executeUpdate"));
    BatchEntity batch = new BatchEntity();
    batch.setId(JOB_ID);

    assertThrows(RatchetTransientStoreException.class, () -> batches.saveBatch(batch));
  }

  @Test
  void resetOrphanJobsTranslatesTransientWriteFailure() {
    MysqlStoreContext ctx = throwingContext("executeUpdate");
    MysqlJobDeleteOperations deletes =
        new MysqlJobDeleteOperations(ctx, new MysqlBusinessKeyReservations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> deletes.resetOrphanJobs(Duration.ofSeconds(30)));
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
        () -> terminals.markJobFailedTerminal(JOB_ID, "boom", 1));
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
        RatchetTransientStoreException.class, () -> auxiliary.releasePermit("api", JOB_ID));
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
