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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;

/** Exercises production lease SQL and driver counts without the shared fixture's retry wrapper. */
class MysqlLeaseContentionTest {
  private static final MetricsCollector METRICS = mock(MetricsCollector.class);

  @Test
  void ownershipWithMatchedRowCounts() throws Exception {
    exercise(false, java.sql.Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  void ownershipWithChangedRowCounts() throws Exception {
    exercise(true, java.sql.Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  void ownershipWithMatchedRowCountsAtRepeatableRead() throws Exception {
    exercise(false, java.sql.Connection.TRANSACTION_REPEATABLE_READ);
  }

  @Test
  void ownershipWithChangedRowCountsAtRepeatableRead() throws Exception {
    exercise(true, java.sql.Connection.TRANSACTION_REPEATABLE_READ);
  }

  private void exercise(boolean affectedRows, int isolation) throws Exception {
    var fixture = new MysqlTestFixture();
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    props.put(
        "jakarta.persistence.jdbc.url",
        container.getJdbcUrl() + "&useAffectedRows=" + affectedRows);
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    props.put("hibernate.connection.pool_size", "12");
    props.put("hibernate.connection.isolation", Integer.toString(isolation));
    var emf = Persistence.createEntityManagerFactory("ratchet-mysql-tck", props);
    String name = "lease-counts-" + UUID.randomUUID();
    try {
      String actualIsolation =
          tx(
              emf,
              em ->
                  (String)
                      em.createNativeQuery("SELECT @@transaction_isolation").getSingleResult());
      assertEquals(
          isolation == java.sql.Connection.TRANSACTION_REPEATABLE_READ
              ? "REPEATABLE-READ"
              : "READ-COMMITTED",
          actualIsolation);
      assertTrue(acquire(emf, name, "owner"));
      Object[] before = row(emf, name);
      assertFalse(acquire(emf, name, "other"));
      boolean renewed = tx(emf, em -> locks(em).renewLock(name, Duration.ofMinutes(1), "other"));
      assertFalse(renewed);
      release(emf, name, "other");
      assertArrayEquals(before, row(emf, name), "A losing caller must not mutate the lease");
      assertTrue(acquire(emf, name, "owner"), "Same owner can extend its live lease");
      tx(
          emf,
          em ->
              em.createNativeQuery(
                      "UPDATE scheduler_lock SET expires_at=NOW(6)-INTERVAL 1 SECOND WHERE lock_name=?")
                  .setParameter(1, name)
                  .executeUpdate());
      assertTrue(acquire(emf, name, "other"), "Expired lease can change owner");
      release(emf, name, "owner");
      assertEquals("other", row(emf, name)[0], "Former owner cannot delete the new lease");
      release(emf, name, "other");
      assertTrue(acquire(emf, name, "owner"), "Released lease can be created again");
      release(emf, name, "owner");
      contend(emf, name);
    } finally {
      tx(
          emf,
          em ->
              em.createNativeQuery("DELETE FROM scheduler_lock WHERE lock_name=?")
                  .setParameter(1, name)
                  .executeUpdate());
      emf.close();
    }
  }

  private void contend(EntityManagerFactory emf, String name) throws Exception {
    var start = new CountDownLatch(1);
    var ready = new CountDownLatch(8);
    var active = new AtomicInteger();
    var acquired = new AtomicInteger();
    var workers = Executors.newFixedThreadPool(8);
    var futures = new ArrayList<java.util.concurrent.Future<?>>();
    try {
      for (int i = 0; i < 8; i++) {
        String owner = "node-" + i;
        futures.add(
            workers.submit(
                () -> {
                  ready.countDown();
                  assertTrue(start.await(10, TimeUnit.SECONDS));
                  for (int attempt = 0; attempt < 80; attempt++) {
                    if (acquireWithRetry(emf, name, owner)) {
                      try {
                        assertEquals(
                            1, active.incrementAndGet(), "Only one node may own the lease");
                        acquired.incrementAndGet();
                        Thread.yield();
                      } finally {
                        active.decrementAndGet();
                        release(emf, name, owner);
                      }
                    }
                  }
                  return null;
                }));
      }
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();
      for (var future : futures) future.get(90, TimeUnit.SECONDS);
      assertTrue(acquired.get() > 0);
      assertEquals(0, active.get());
    } finally {
      start.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static boolean acquireWithRetry(EntityManagerFactory emf, String name, String owner) {
    // The shared fixture must not hide failures. Retry only acquisition conflicts and start
    // a fresh transaction each time, matching the singleton-lease caller's bounded recovery.
    for (int attempt = 1; ; attempt++) {
      try {
        return acquire(emf, name, owner);
      } catch (RatchetTransientStoreException failure) {
        if (attempt == 3) throw failure;
      }
    }
  }

  private static boolean acquire(EntityManagerFactory emf, String name, String owner) {
    return tx(emf, em -> locks(em).tryLock(name, Duration.ofMinutes(5), owner));
  }

  private static void release(EntityManagerFactory emf, String name, String owner) {
    tx(
        emf,
        em -> {
          locks(em).unlock(name, owner);
          return null;
        });
  }

  private static Object[] row(EntityManagerFactory emf, String name) {
    return tx(
        emf,
        em ->
            (Object[])
                em.createNativeQuery(
                        "SELECT owner_node,locked_at,expires_at FROM scheduler_lock WHERE lock_name=?")
                    .setParameter(1, name)
                    .getSingleResult());
  }

  private static MysqlNodeLockOperations locks(EntityManager em) {
    return new MysqlNodeLockOperations(new MysqlStoreContext(em, METRICS));
  }

  private static <T> T tx(EntityManagerFactory emf, Function<EntityManager, T> work) {
    var em = emf.createEntityManager();
    var transaction = em.getTransaction();
    try {
      transaction.begin();
      T result = work.apply(em);
      transaction.commit();
      return result;
    } finally {
      if (transaction.isActive()) transaction.rollback();
      em.close();
    }
  }
}
