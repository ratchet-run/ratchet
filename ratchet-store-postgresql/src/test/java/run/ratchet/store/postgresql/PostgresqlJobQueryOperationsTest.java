package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.store.query.JobQueryCursor;

class PostgresqlJobQueryOperationsTest {

  @Test
  void searchJobsIgnoresCursorWithMalformedInstantSortValue() {
    String cursor =
        new JobQueryCursor(
                JobQuerySortField.CREATED_AT,
                "not-an-instant",
                UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000465"))
            .encode();
    NativeSqlRecorder recorder = new NativeSqlRecorder();
    PostgresqlStoreContext ctx = new PostgresqlStoreContext(recorder.entityManager());
    PostgresqlJobQueryOperations operations =
        new PostgresqlJobQueryOperations(ctx, new PostgresqlTagOperations(ctx));

    assertDoesNotThrow(
        () -> operations.searchJobs(JobFilter.builder().cursor(cursor).build(), 10, 7));
    assertTrue(
        recorder.lastSql().contains("OFFSET 7"),
        "Malformed cursors should leave offset pagination in place");
  }

  @Test
  void searchJobsWithArchiveCursorFiltersBothUnionBranches() {
    String cursor =
        new JobQueryCursor(
                JobQuerySortField.CREATED_AT,
                "2026-01-01T00:00:00Z",
                UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000465"))
            .encode();
    NativeSqlRecorder recorder = new NativeSqlRecorder();
    PostgresqlStoreContext ctx = new PostgresqlStoreContext(recorder.entityManager());
    PostgresqlJobQueryOperations operations =
        new PostgresqlJobQueryOperations(ctx, new PostgresqlTagOperations(ctx));

    operations.searchJobs(JobFilter.builder().includeArchived(true).cursor(cursor).build(), 10, 25);

    String sql = recorder.lastSql();
    assertTrue(
        sql.contains("c.created_at < ? OR (c.created_at = ? AND c.job_id > ?)"),
        "Live branch should apply the keyset cursor");
    assertTrue(
        sql.contains(
            "a.original_created_at < ? OR (a.original_created_at = ? AND a.original_job_id > ?)"),
        "Archive branch should apply the keyset cursor");
    assertFalse(sql.contains("OFFSET 25"), "Cursor pagination should not combine with offset");
    assertTrue(sql.contains("OFFSET 0"), "Cursor pagination should reset offset");
  }

  private static final class NativeSqlRecorder {
    private final List<String> sql = new ArrayList<>();

    EntityManager entityManager() {
      return (EntityManager)
          Proxy.newProxyInstance(
              EntityManager.class.getClassLoader(),
              new Class<?>[] {EntityManager.class},
              (proxy, method, args) -> {
                if ("createNativeQuery".equals(method.getName())) {
                  sql.add((String) args[0]);
                  return emptyResultQuery();
                }
                throw new UnsupportedOperationException(method.getName());
              });
    }

    String lastSql() {
      assertFalse(sql.isEmpty(), "Expected at least one native query");
      return sql.get(sql.size() - 1);
    }
  }

  private static Query emptyResultQuery() {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if ("getResultList".equals(method.getName())) {
                return List.of();
              }
              return switch (method.getName()) {
                case "setParameter", "setFirstResult", "setMaxResults" -> proxy;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
