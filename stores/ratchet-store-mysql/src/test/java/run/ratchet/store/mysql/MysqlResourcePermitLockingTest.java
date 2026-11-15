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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.Query;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

class MysqlResourcePermitLockingTest {
  private final MysqlTestFixture fixture = new MysqlTestFixture();
  private EntityManagerFactory emf;

  private EntityManagerFactory newFactory() {
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    props.put("jakarta.persistence.jdbc.url", container.getJdbcUrl());
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    props.put("hibernate.connection.isolation", "4");
    return Persistence.createEntityManagerFactory("ratchet-mysql-tck", props);
  }

  private MysqlJobStoreImpl store(EntityManager em) {
    var store =
        new MysqlJobStoreImpl(() -> em, mock(MetricsCollector.class), RatchetOptions.defaults());
    store.checkIsolationLevel();
    return store;
  }

  @AfterEach
  void cleanup() {
    if (emf != null) emf.close();
    fixture.cleanupStore();
  }

  private List<UUID> seed() {
    fixture.cleanupStore();
    emf = newFactory();
    var em = emf.createEntityManager();
    try {
      var store = store(em);
      em.getTransaction().begin();
      var ids =
          List.of(
              store.create(fixture.newPendingJob()).getId(),
              store.create(fixture.newPendingJob()).getId());
      store.configureResource("r-a", 3, 100, "test");
      store.configureResource("r-b", 3, 100, "test");
      em.flush();
      for (UUID id : ids) {
        em.createNativeQuery(
                """
                UPDATE scheduler_job_queue SET status = 'RUNNING', claim_seq = 7 WHERE job_id = ?
                """)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .executeUpdate();
      }
      em.getTransaction().commit();
      return ids;
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
    }
  }

  private void begin(EntityManager em) {
    em.getTransaction().begin();
    em.createNativeQuery("SET SESSION innodb_lock_wait_timeout = 3").executeUpdate();
  }

  @Test
  void differentResourcesReadBeforeEitherInserts() throws Exception {
    var ids = seed();
    var readers = new CountDownLatch(2);
    var workers = Executors.newFixedThreadPool(2);
    try {
      var first = workers.submit(() -> acquireAfterBothRead("r-a", ids.get(0), readers));
      var second = workers.submit(() -> acquireAfterBothRead("r-b", ids.get(1), readers));
      assertEquals(AcquireResult.ACQUIRED, first.get(10, TimeUnit.SECONDS));
      assertEquals(AcquireResult.ACQUIRED, second.get(10, TimeUnit.SECONDS));
    } finally {
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private AcquireResult acquireAfterBothRead(String resource, UUID id, CountDownLatch readers) {
    var em = emf.createEntityManager();
    try {
      begin(em);
      // Intercept the real query result to ensure both transactions have read before inserts.
      var observed =
          observe(
              em,
              sql -> sql.contains("SELECT id FROM scheduler_resource_permit"),
              () -> {
                readers.countDown();
                await(readers);
              },
              true);
      AcquireResult acquired =
          new MysqlAuxiliaryOperations(new MysqlStoreContext(observed, null))
              .tryAcquirePermit(resource, id, 7L, "live");
      em.getTransaction().commit();
      return acquired;
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cleanupWaitsForAcquireThenDeletesOnlyStalePermit(boolean missingNode) throws Exception {
    var ids = seed();
    var seed = emf.createEntityManager();
    try {
      begin(seed);
      assertEquals(
          AcquireResult.ACQUIRED, store(seed).tryAcquirePermit("r-a", ids.get(0), 7L, "stale"));
      seed.getTransaction().commit();
    } finally {
      if (seed.getTransaction().isActive()) seed.getTransaction().rollback();
      seed.close();
    }
    var holder = emf.createEntityManager();
    var workers = Executors.newSingleThreadExecutor();
    var locking = new CountDownLatch(1);
    try {
      begin(holder);
      assertEquals(
          AcquireResult.ACQUIRED, store(holder).tryAcquirePermit("r-a", ids.get(1), 7L, "live"));
      var cleanup =
          workers.submit(
              () -> {
                var em = emf.createEntityManager();
                try {
                  begin(em);
                  var observed =
                      observe(
                          em,
                          sql -> sql.contains("FROM scheduler_resource_limit"),
                          locking::countDown,
                          false);
                  var operations =
                      new MysqlAuxiliaryOperations(new MysqlStoreContext(observed, null));
                  int deleted =
                      missingNode
                          ? operations.cleanupPermitsOfMissingNodes(Instant.now().plusSeconds(60))
                          : operations.cleanupOrphanedPermits(List.of("stale"));
                  em.getTransaction().commit();
                  return deleted;
                } finally {
                  if (em.getTransaction().isActive()) em.getTransaction().rollback();
                  em.close();
                }
              });
      assertTrue(locking.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> cleanup.get(150, TimeUnit.MILLISECONDS));
      holder.getTransaction().commit();
      assertEquals(1, cleanup.get(5, TimeUnit.SECONDS));
      begin(holder);
      assertEquals(
          List.of("live"),
          holder
              .createNativeQuery(
                  "SELECT node_id FROM scheduler_resource_permit WHERE resource_name = 'r-a'")
              .getResultList());
      holder.getTransaction().commit();
    } finally {
      if (holder.getTransaction().isActive()) holder.getTransaction().rollback();
      holder.close();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void bulkDeleteAndAcquireDoNotDeadlock(boolean deleteFirst) throws Exception {
    var ids = seed();
    var holder = emf.createEntityManager();
    var workers = Executors.newSingleThreadExecutor();
    try {
      begin(holder);
      assertEquals(
          AcquireResult.ACQUIRED, store(holder).tryAcquirePermit("r-a", ids.get(0), 7L, "old"));
      holder.getTransaction().commit();
      begin(holder);
      if (deleteFirst) assertEquals(1, store(holder).deleteJobsByIds(List.of(ids.get(0))));
      else
        assertEquals(
            AcquireResult.ACQUIRED, store(holder).tryAcquirePermit("r-a", ids.get(1), 7L, "live"));
      var started = new CountDownLatch(1);
      var other =
          workers.submit(
              () -> {
                var em = emf.createEntityManager();
                try {
                  begin(em);
                  started.countDown();
                  if (deleteFirst)
                    assertEquals(
                        AcquireResult.ACQUIRED,
                        store(em).tryAcquirePermit("r-a", ids.get(1), 7L, "live"));
                  else assertEquals(1, store(em).deleteJobsByIds(List.of(ids.get(0))));
                  em.getTransaction().commit();
                  return true;
                } finally {
                  if (em.getTransaction().isActive()) em.getTransaction().rollback();
                  em.close();
                }
              });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      // These paths may block on a permit key but must complete after the holder commits.
      try {
        assertTrue(other.get(150, TimeUnit.MILLISECONDS));
      } catch (TimeoutException expected) {
        // A bounded wait allows blocking but exposes a lock-order cycle after holder commit.
      }
      holder.getTransaction().commit();
      assertTrue(other.get(5, TimeUnit.SECONDS));
      begin(holder);
      assertEquals(
          List.of("live"),
          holder
              .createNativeQuery(
                  "SELECT node_id FROM scheduler_resource_permit WHERE resource_name = 'r-a'")
              .getResultList());
      holder.getTransaction().commit();
    } finally {
      if (holder.getTransaction().isActive()) holder.getTransaction().rollback();
      holder.close();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static EntityManager observe(
      EntityManager em, Predicate<String> match, Runnable action, boolean after) {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              try {
                Object result = method.invoke(em, args);
                if (method.getName().equals("createNativeQuery") && match.test((String) args[0])) {
                  Query query = (Query) result;
                  return Proxy.newProxyInstance(
                      Query.class.getClassLoader(),
                      new Class<?>[] {Query.class},
                      (p, m, a) -> {
                        try {
                          if (m.getName().equals("getResultList") && !after) action.run();
                          Object value = m.invoke(query, a);
                          if (m.getName().equals("getResultList") && after) action.run();
                          return value == query ? p : value;
                        } catch (InvocationTargetException e) {
                          throw e.getCause();
                        }
                      });
                }
                return result;
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }
}
