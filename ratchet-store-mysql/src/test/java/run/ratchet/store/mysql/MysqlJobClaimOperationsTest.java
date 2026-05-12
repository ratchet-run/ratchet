package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.NodeTagFilter;

class MysqlJobClaimOperationsTest {

  @Test
  void claimSelectColumnOrderMatchesNamedRowMapping() {
    List<String> columns = MysqlJobClaimOperations.claimSelectColumnNames();

    assertEquals(
        List.of(
            "job_id",
            "status",
            "job_type",
            "priority",
            "scheduled_time",
            "version",
            "timeout_sec",
            "picked_by",
            "picked_at",
            "business_key",
            "attempts",
            "max_retries"),
        columns);
    assertEquals(
        Map.ofEntries(
            Map.entry("job_id", 0),
            Map.entry("status", 1),
            Map.entry("job_type", 2),
            Map.entry("priority", 3),
            Map.entry("scheduled_time", 4),
            Map.entry("version", 5),
            Map.entry("timeout_sec", 6),
            Map.entry("picked_by", 7),
            Map.entry("picked_at", 8),
            Map.entry("business_key", 9),
            Map.entry("attempts", 10),
            Map.entry("max_retries", 11)),
        MysqlJobClaimOperations.claimSelectColumnIndexes());
  }

  @Test
  void claimDueRecurringReturnsEmptyForNonPositiveLimit() {
    MysqlJobClaimOperations operations = new MysqlJobClaimOperations(null, null);

    assertTrue(operations.claimDueRecurring(0, "node-1", null).isEmpty());
    assertTrue(operations.claimDueRecurring(-1, "node-1", null).isEmpty());
  }

  @Test
  void claimDueRecurringExcludesMastersWithActiveQueueRows() {
    List<String> sqlStatements = new ArrayList<>();
    MysqlJobClaimOperations operations =
        new MysqlJobClaimOperations(
            new MysqlStoreContext(entityManagerReturningEmpty(sqlStatements), null), null);

    assertTrue(operations.claimDueRecurring(10, "node-1", NodeTagFilter.NONE).isEmpty());

    assertTrue(sqlStatements.get(0).contains("AND q.job_id IS NULL"));
  }

  private static EntityManager entityManagerReturningEmpty(List<String> sqlStatements) {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                sqlStatements.add((String) args[0]);
                return queryReturningEmpty();
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query queryReturningEmpty() {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "getResultList" -> List.of();
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
