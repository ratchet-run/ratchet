package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertThrows;

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

class PostgresqlExceptionTranslationTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000031");

  @Test
  void archiveJobTranslatesInsertDeadlock() {
    EntityManager em =
        entityManager(
            queryReturning(Collections.singletonList(terminalRow())),
            queryReturning(List.of()),
            queryThrowingOnExecute());
    var ctx = new PostgresqlStoreContext(em);
    var tags = new PostgresqlTagOperations(ctx);
    var reads = new PostgresqlJobReadOperations(ctx, tags);
    var archives = new PostgresqlArchiveOperations(ctx, reads);
    JobEntity input = new JobEntity();
    input.setId(JOB_ID);

    assertThrows(
        RatchetTransientStoreException.class,
        () -> archives.archiveJob(input, "retention", "test"));
  }

  @Test
  void businessKeyReservationsTranslateDeadlock() {
    var reservations =
        new PostgresqlBusinessKeyReservations(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            reservations.insertReservation(
                "key", JOB_ID, PostgresqlBusinessKeyReservations.OWNER_TABLE_QUEUE));
  }

  @Test
  void deleteOperationsTranslateDeadlock() {
    var deletes =
        new PostgresqlJobDeleteOperations(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())),
            new PostgresqlBusinessKeyReservations(
                new PostgresqlStoreContext(entityManager(queryReturningUpdate(1)))));

    assertThrows(
        RatchetTransientStoreException.class, () -> deletes.resetOrphanJobsForNode("node"));
  }

  @Test
  void readOperationsTranslateDeadlock() {
    var ctx = new PostgresqlStoreContext(entityManager(queryThrowingOnResults()));
    var reads = new PostgresqlJobReadOperations(ctx, new PostgresqlTagOperations(ctx));

    assertThrows(RatchetTransientStoreException.class, () -> reads.findById(JOB_ID));
  }

  @Test
  void queryOperationsTranslateSearchDeadlock() {
    var ctx = new PostgresqlStoreContext(entityManager(queryThrowingOnResults()));
    var queries = new PostgresqlJobQueryOperations(ctx, new PostgresqlTagOperations(ctx));

    assertThrows(RatchetTransientStoreException.class, () -> queries.searchJobs(null, 10, 0));
  }

  @Test
  void queryOperationsTranslateCountDeadlock() {
    var ctx = new PostgresqlStoreContext(entityManager(queryThrowingOnSingleResult()));
    var queries = new PostgresqlJobQueryOperations(ctx, new PostgresqlTagOperations(ctx));

    assertThrows(RatchetTransientStoreException.class, () -> queries.countJobs(null));
  }

  @Test
  void statusTransitionsTranslateDeadlock() {
    var transitions =
        new PostgresqlJobStatusTransitions(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class, () -> transitions.tryPickUpJob(JOB_ID, "node"));
  }

  @Test
  void terminalOperationsTranslateDeadlock() {
    var ctx = new PostgresqlStoreContext(entityManager(queryThrowingOnExecute()));
    var terminals =
        new PostgresqlJobTerminalOperations(
            ctx, new PostgresqlBusinessKeyReservations(ctx), new PostgresqlBatchOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> terminals.scheduleJobRetry(JOB_ID, "boom", Instant.now(), 1));
  }

  @Test
  void tagOperationsTranslateDeadlock() {
    var tags =
        new PostgresqlTagOperations(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(RatchetTransientStoreException.class, () -> tags.deleteTagsByJobId(JOB_ID));
  }

  @Test
  void nodeLockOperationsTranslateDeadlock() {
    var locks =
        new PostgresqlNodeLockOperations(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> locks.tryLock("scheduler", Duration.ofSeconds(30), "node-1"));
  }

  @Test
  void signalOperationsTranslateDeadlock() {
    var signals =
        new PostgresqlSignalOperations(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            signals.deliverSignalByKey(
                "approval", "{}", "json", "APPROVED", null, "node-1", Instant.EPOCH, "delivery-1"));
  }

  @Test
  void resourcePermitReleaseTranslatesDeadlock() {
    var auxiliary =
        new PostgresqlAuxiliaryOperations(
            new PostgresqlStoreContext(entityManager(queryThrowingOnExecute())));

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
    return new PersistenceException("deadlock", new SQLException("deadlock", "40P01"));
  }

  private static Object[] terminalRow() {
    Object[] row = new Object[PostgresqlJobRowMapper.HYDRATION_COL_COUNT];
    Instant now = Instant.parse("2026-05-12T14:30:00Z");
    row[0] = JOB_ID;
    row[1] = JobExecutionType.SINGLE.name();
    row[2] = JobPriority.NORMAL.ordinal();
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
