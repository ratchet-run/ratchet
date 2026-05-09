package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class MysqlBusinessKeyReservationsTest {

  @Test
  void insertReservationRequiresBusinessKey() {
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(null, null));

    assertThrows(
        NullPointerException.class,
        () ->
            reservations.insertReservation(
                null,
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE));
  }

  @Test
  void bindInsertRequiresBusinessKeyBeforeBindingAnyParameters() {
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(null, null));
    JobEntity job = new JobEntity();
    job.setId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    job.setJobType(JobExecutionType.SINGLE);
    AtomicInteger parameterBindings = new AtomicInteger();
    Query query = queryCountingParameterBindings(parameterBindings);

    assertThrows(
        NullPointerException.class,
        () -> reservations.bindInsert(query, job, Timestamp.from(Instant.EPOCH)));
    assertEquals(0, parameterBindings.get());
  }

  private static Query queryCountingParameterBindings(AtomicInteger parameterBindings) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if (method.getName().equals("setParameter")) {
                parameterBindings.incrementAndGet();
                return proxy;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
