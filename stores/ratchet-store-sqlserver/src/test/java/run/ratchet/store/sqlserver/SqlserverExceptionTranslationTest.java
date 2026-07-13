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
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class SqlserverExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000031");

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
    var deletes =
        new SqlserverJobDeleteOperations(
            new SqlserverStoreContext(entityManager(queryThrowingOnExecute())),
            new SqlserverBusinessKeyReservations(
                new SqlserverStoreContext(entityManager(queryReturningUpdate(1)))));

    assertThrows(
        RatchetTransientStoreException.class, () -> deletes.resetOrphanJobsForNode("node"));
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
    var ctx = new SqlserverStoreContext(entityManager(queryThrowingOnExecute()));
    var terminals =
        new SqlserverJobTerminalOperations(
            ctx, new SqlserverBusinessKeyReservations(ctx), new SqlserverBatchOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> terminals.scheduleJobRetry(JOB_ID, "boom", Instant.now(), 1));
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
        RatchetTransientStoreException.class, () -> auxiliary.releasePermit("api", JOB_ID));
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
