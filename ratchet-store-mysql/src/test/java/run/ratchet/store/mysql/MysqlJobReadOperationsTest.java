package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MysqlJobReadOperationsTest {

  @Test
  void findEarliestRecurringNextFireAcceptsLocalDateTimeDriverValues() {
    LocalDateTime nextFire = LocalDateTime.parse("2026-05-12T14:30:00");
    MysqlJobReadOperations reads = readsReturning(nextFire);

    Optional<Instant> result = reads.findEarliestRecurringNextFire();

    assertEquals(Optional.of(Instant.parse("2026-05-12T14:30:00Z")), result);
  }

  private static MysqlJobReadOperations readsReturning(Object value) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName())) {
                    return queryReturning(value);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    MysqlStoreContext ctx = new MysqlStoreContext(em, null);
    return new MysqlJobReadOperations(ctx, new MysqlJobRowMapper(), new MysqlTagOperations(ctx));
  }

  private static Query queryReturning(Object value) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if ("getResultList".equals(method.getName())) {
                return List.of(value);
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
