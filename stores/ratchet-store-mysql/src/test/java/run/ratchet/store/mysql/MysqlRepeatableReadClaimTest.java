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
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

/** Real claim transactions must not fence concurrent enqueue at MySQL's default isolation. */
class MysqlRepeatableReadClaimTest {
  private final MysqlTestFixture fixture = new MysqlTestFixture();
  private EntityManagerFactory emf;

  @BeforeEach
  void setup() {
    fixture.cleanupStore();
    emf = newFactory(4);
  }

  private EntityManagerFactory newFactory(int isolation) {
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    props.put("jakarta.persistence.jdbc.url", container.getJdbcUrl());
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    props.put("hibernate.connection.isolation", Integer.toString(isolation));
    return Persistence.createEntityManagerFactory("ratchet-mysql-tck", props);
  }

  @AfterEach
  void cleanup() {
    if (emf != null) emf.close();
    fixture.cleanupStore();
  }

  private MysqlJobStoreImpl store(EntityManager em) {
    var options = RatchetOptions.defaults();
    var store = new MysqlJobStoreImpl(() -> em, mock(MetricsCollector.class), options);
    store.checkIsolationLevel();
    return store;
  }

  @Test
  void emptyClaimDoesNotBlockEnqueue() throws Exception {
    enqueueWhileClaimIsOpen(false);
  }

  @Test
  void populatedClaimDoesNotBlockEnqueue() throws Exception {
    enqueueWhileClaimIsOpen(true);
  }

  @Test
  void emptyRecurringClaimDoesNotBlockRegistration() throws Exception {
    var workers = Executors.newSingleThreadExecutor();
    var em = emf.createEntityManager();
    try {
      var store = store(em);
      em.getTransaction().begin();
      assertTrue(
          store.claimDueRecurring(10, "reader", run.ratchet.api.NodeTagFilter.NONE).isEmpty());
      var insert =
          workers.submit(
              () -> {
                var writer = emf.createEntityManager();
                try {
                  var writerStore = store(writer);
                  writer.getTransaction().begin();
                  writer
                      .createNativeQuery("SET SESSION innodb_lock_wait_timeout=1")
                      .executeUpdate();
                  var definition =
                      new run.ratchet.store.spi.RecurringJobDefinition(
                          java.util.UUID.randomUUID(),
                          "0 * * * * ?",
                          "UTC",
                          java.time.Instant.now().minusSeconds(1),
                          false,
                          null,
                          0,
                          0,
                          run.ratchet.api.BackoffPolicy.FIXED,
                          100,
                          60,
                          fixture.newPendingJob().getPayload(),
                          null,
                          null,
                          null,
                          null,
                          null,
                          java.time.Instant.now(),
                          null,
                          false,
                          run.ratchet.api.RecurringMisfirePolicy.defaults());
                  var id = writerStore.createRecurring(definition);
                  writer.getTransaction().commit();
                  return id;
                } finally {
                  if (writer.getTransaction().isActive()) writer.getTransaction().rollback();
                  writer.close();
                }
              });
      assertNotNull(insert.get(5, TimeUnit.SECONDS));
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void oldSnapshotCannotExceedResourceCapacity() {
    var permits =
        fixture.store().capability(run.ratchet.store.spi.ResourcePermitStore.class).orElseThrow();
    permits.configureResource("capacity-one", 1, 100, "test");
    var winner = fixture.store().create(fixture.newPendingJob()).getId();
    var loser = fixture.store().create(fixture.newPendingJob()).getId();
    var em = emf.createEntityManager();
    try {
      var store = store(em);
      em.getTransaction().begin();
      em.createNativeQuery("SELECT COUNT(*) FROM scheduler_resource_permit").getSingleResult();
      assertTrue(permits.tryAcquirePermit("capacity-one", winner, "other-node"));
      assertFalse(
          store.tryAcquirePermit("capacity-one", loser, "stale-node"),
          "A pre-existing snapshot must not hide a committed permit");
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
    }
  }

  @Test
  void skipsLockedHeadAndFindsLaterCandidates() {
    var firstJob = fixture.newPendingJob();
    firstJob.setScheduledTime(java.time.Instant.now().minusSeconds(10));
    var first = fixture.store().create(firstJob);
    var later = fixture.store().create(fixture.newPendingJob());
    var locked = emf.createEntityManager();
    var reader = emf.createEntityManager();
    try {
      locked.getTransaction().begin();
      locked
          .createNativeQuery("SELECT job_id FROM scheduler_job_queue WHERE job_id=? FOR UPDATE")
          .setParameter(
              1, run.ratchet.store.mysql.converter.UuidByteArrayConverter.toBytes(first.getId()))
          .getResultList();
      var store = store(reader);
      reader.getTransaction().begin();
      var claims = store.claimNextBatchOptimized(JobExecutionType.SINGLE, 1, "reader");
      assertEquals(1, claims.size());
      assertEquals(later.getId(), claims.get(0).id());
    } finally {
      if (reader.getTransaction().isActive()) reader.getTransaction().rollback();
      if (locked.getTransaction().isActive()) locked.getTransaction().rollback();
      reader.close();
      locked.close();
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"status='PAUSED'", "scheduled_time=NOW(3)+INTERVAL 1 HOUR", "status='RUNNING'"})
  void rechecksEligibilityAfterAnOlderSnapshot(String change) {
    var job = fixture.store().create(fixture.newPendingJob());
    var reader = emf.createEntityManager();
    var writer = emf.createEntityManager();
    try {
      var store = store(reader);
      reader.getTransaction().begin();
      reader.createNativeQuery("SELECT job_id FROM scheduler_job_queue").getResultList();
      writer.getTransaction().begin();
      writer
          .createNativeQuery("UPDATE scheduler_job_queue SET " + change + " WHERE job_id=?")
          .setParameter(
              1, run.ratchet.store.mysql.converter.UuidByteArrayConverter.toBytes(job.getId()))
          .executeUpdate();
      writer.getTransaction().commit();
      assertTrue(store.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "reader").isEmpty());
    } finally {
      if (reader.getTransaction().isActive()) reader.getTransaction().rollback();
      if (writer.getTransaction().isActive()) writer.getTransaction().rollback();
      reader.close();
      writer.close();
    }
  }

  @Test
  void claimMetadataComesFromLockedCurrentRow() {
    var job = fixture.store().create(fixture.newPendingJob());
    var reader = emf.createEntityManager();
    var writer = emf.createEntityManager();
    try {
      var store = store(reader);
      reader.getTransaction().begin();
      reader.createNativeQuery("SELECT job_id FROM scheduler_job_queue").getResultList();
      writer.getTransaction().begin();
      writer
          .createNativeQuery("UPDATE scheduler_job_queue SET attempts=5, version=7 WHERE job_id=?")
          .setParameter(
              1, run.ratchet.store.mysql.converter.UuidByteArrayConverter.toBytes(job.getId()))
          .executeUpdate();
      writer.getTransaction().commit();
      var claims = store.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "reader");
      assertEquals(1, claims.size());
      assertEquals(5, claims.get(0).attempts());
      assertEquals(7, claims.get(0).version());
    } finally {
      if (reader.getTransaction().isActive()) reader.getTransaction().rollback();
      if (writer.getTransaction().isActive()) writer.getTransaction().rollback();
      reader.close();
      writer.close();
    }
  }

  @Test
  void concurrentInsertCannotDuplicateClaimAcrossReadCommittedPages() {
    var jobs = new java.util.ArrayList<run.ratchet.store.entity.JobEntity>();
    for (int i = 0; i < 3; i++) {
      var job = fixture.newPendingJob();
      job.setScheduledTime(java.time.Instant.now().minusSeconds(60 - i * 20));
      jobs.add(fixture.store().create(job));
    }
    var rc = newFactory(2);
    var blocker = rc.createEntityManager();
    var reader = rc.createEntityManager();
    try {
      blocker.getTransaction().begin();
      blocker
          .createNativeQuery("SELECT job_id FROM scheduler_job_queue WHERE job_id=? FOR UPDATE")
          .setParameter(
              1,
              run.ratchet.store.mysql.converter.UuidByteArrayConverter.toBytes(jobs.get(0).getId()))
          .getResultList();
      var injected = new java.util.concurrent.atomic.AtomicBoolean();
      EntityManager observed =
          afterFirstPointLookup(
              reader,
              () -> {
                var inserted = fixture.newPendingJob();
                inserted.setPriority(run.ratchet.api.JobPriority.CRITICAL);
                fixture.store().create(inserted);
                injected.set(true);
              });
      var store = store(observed);
      reader.getTransaction().begin();
      var claims = store.claimNextBatchOptimized(JobExecutionType.SINGLE, 2, "reader");
      assertTrue(injected.get());
      assertEquals(
          java.util.List.of(jobs.get(1).getId(), jobs.get(2).getId()),
          claims.stream().map(run.ratchet.store.dto.JobClaimDto::id).toList());
    } finally {
      if (reader.getTransaction().isActive()) reader.getTransaction().rollback();
      if (blocker.getTransaction().isActive()) blocker.getTransaction().rollback();
      reader.close();
      blocker.close();
      rc.close();
    }
  }

  @org.junit.jupiter.api.RepeatedTest(3)
  void concurrentSmallQueueClaimsDoNotScanOtherOwnersRows() throws Exception {
    for (int i = 0; i < 16; i++) fixture.store().create(fixture.newPendingJob());
    var workers = Executors.newFixedThreadPool(2);
    var bothLocked = new java.util.concurrent.CyclicBarrier(2);
    var futures =
        new java.util.ArrayList<java.util.concurrent.Future<java.util.List<java.util.UUID>>>();
    try {
      for (int i = 0; i < 2; i++) {
        String node = "small-queue-" + i;
        futures.add(
            workers.submit(
                () -> {
                  var em = emf.createEntityManager();
                  try {
                    var observed =
                        afterFirstPointLookup(
                            em,
                            () -> {
                              try {
                                bothLocked.await(10, TimeUnit.SECONDS);
                              } catch (Exception e) {
                                throw new AssertionError(e);
                              }
                            });
                    var store = store(observed);
                    em.getTransaction().begin();
                    em.createNativeQuery("SET SESSION innodb_lock_wait_timeout=3").executeUpdate();
                    var claims = store.claimNextBatchOptimized(JobExecutionType.SINGLE, 8, node);
                    em.getTransaction().commit();
                    assertEquals(8, claims.size());
                    return claims.stream().map(run.ratchet.store.dto.JobClaimDto::id).toList();
                  } finally {
                    if (em.getTransaction().isActive()) em.getTransaction().rollback();
                    em.close();
                  }
                }));
      }
      var ids = new java.util.HashSet<java.util.UUID>();
      try {
        for (var future : futures) ids.addAll(future.get(20, TimeUnit.SECONDS));
      } catch (Exception failure) {
        var container = fixture.container();
        try (var connection =
                java.sql.DriverManager.getConnection(
                    container.getJdbcUrl(), "root", container.getPassword());
            var statement = connection.createStatement();
            var result = statement.executeQuery("SHOW ENGINE INNODB STATUS")) {
          if (result.next()) System.err.println(result.getString(3));
        } catch (Exception diagnosticFailure) {
          failure.addSuppressed(diagnosticFailure);
        }
        throw failure;
      }
      assertEquals(16, ids.size());
    } finally {
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  private static EntityManager afterFirstPointLookup(EntityManager em, Runnable action) {
    var invoked = new java.util.concurrent.atomic.AtomicBoolean();
    return (EntityManager)
        java.lang.reflect.Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              Object result = invoke(method, em, args);
              if (method.getName().equals("createNativeQuery")
                  && ((String) args[0]).contains("FORCE INDEX (PRIMARY)")) {
                var query = (jakarta.persistence.Query) result;
                return java.lang.reflect.Proxy.newProxyInstance(
                    jakarta.persistence.Query.class.getClassLoader(),
                    new Class<?>[] {jakarta.persistence.Query.class},
                    (qp, qm, qa) -> {
                      Object qr = invoke(qm, query, qa);
                      if (qm.getName().equals("getResultList")
                          && qr instanceof java.util.List<?> rows
                          && !rows.isEmpty()
                          && invoked.compareAndSet(false, true)) action.run();
                      return qr == query ? qp : qr;
                    });
              }
              return result;
            });
  }

  private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (java.lang.reflect.InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private void enqueueWhileClaimIsOpen(boolean populated) throws Exception {
    if (populated) fixture.store().create(fixture.newPendingJob());
    var workers = Executors.newSingleThreadExecutor();
    var em = emf.createEntityManager();
    try {
      var store = store(em);
      em.getTransaction().begin();
      assertEquals(
          "REPEATABLE-READ",
          em.createNativeQuery("SELECT @@transaction_isolation").getSingleResult());
      var claims = store.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "claiming-node");
      assertEquals(populated ? 1 : 0, claims.size());
      var enqueue =
          workers.submit(
              () -> {
                var writer = emf.createEntityManager();
                try {
                  var writerStore = store(writer);
                  writer.getTransaction().begin();
                  writer
                      .createNativeQuery("SET SESSION innodb_lock_wait_timeout=1")
                      .executeUpdate();
                  var job = writerStore.create(fixture.newPendingJob());
                  writer.getTransaction().commit();
                  return job.getId();
                } finally {
                  if (writer.getTransaction().isActive()) writer.getTransaction().rollback();
                  writer.close();
                }
              });
      assertNotNull(
          enqueue.get(5, TimeUnit.SECONDS),
          "Enqueue must finish before the claim transaction commits");
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
