package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MysqlJobRecurringAndResetOperationsTest {

  @Test
  void cancelOrphanedRecurringAnnotationJobs_updatesCandidatesInOneStatement() {
    UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
    List<String> sqlStatements = new ArrayList<>();
    EntityManager em = recordingEntityManager(sqlStatements, List.of(first, second));
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(em, null));
    MysqlJobRecurringAndResetOperations recurring =
        new MysqlJobRecurringAndResetOperations(new MysqlStoreContext(em, null), reservations);

    int cancelled =
        recurring.cancelOrphanedRecurringAnnotationJobs(
            Set.of("still-registered"), Instant.parse("2026-05-10T12:00:00Z"));

    assertEquals(2, cancelled);
    assertEquals(3, sqlStatements.size(), "select, bulk update, and bulk reservation cleanup");
    assertTrue(sqlStatements.get(1).contains("job_id IN (?,?)"));
    assertTrue(sqlStatements.get(2).contains("owner_job_id IN (?,?)"));
    assertTrue(sqlStatements.get(2).contains("terminal_status = 'CANCELED'"));
  }

  private static EntityManager recordingEntityManager(List<String> sqlStatements, List<UUID> ids) {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName()) && args != null) {
                String sql = ((String) args[0]).stripLeading();
                sqlStatements.add(sql);
                if (sql.startsWith("SELECT")) {
                  return queryReturningRows(ids);
                }
                return queryReturningUpdateCount(2);
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query queryReturningRows(List<UUID> rows) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "getResultList" -> rows;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  private static Query queryReturningUpdateCount(int count) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "executeUpdate" -> count;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
