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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

class MysqlBatchSweepLockingTest {
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

  @ParameterizedTest
  @ValueSource(strings = {"pause", "resume", "cancel"})
  void sweepDoesNotWaitForUnrelatedPendingJob(String operation) {
    sweep(operation, false);
  }

  @Test
  void pauseSeesPendingChildCommittedAfterSnapshot() {
    sweep("pause", true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"pause", "resume", "cancel"})
  void snapshotPredatingBatchMustRetryInNewTransaction(String operation) {
    fixture.cleanupStore();
    emf = newFactory();
    var reader = emf.createEntityManager();
    var writer = emf.createEntityManager();
    try {
      var readerStore = store(reader);
      var writerStore = store(writer);
      reader.getTransaction().begin();
      reader.createNativeQuery("SELECT COUNT(*) FROM scheduler_job").getSingleResult();
      writer.getTransaction().begin();
      UUID parent = writerStore.create(fixture.newPendingJob()).getId();
      for (int i = 0; i < 3; i++) {
        UUID child = writerStore.create(fixture.newPendingJob()).getId();
        writer
            .createNativeQuery(
                """
                UPDATE scheduler_job SET depends_on = ?, job_type = 'BATCH_CHILD' WHERE job_id = ?
                """)
            .setParameter(1, UuidByteArrayConverter.toBytes(parent))
            .setParameter(2, UuidByteArrayConverter.toBytes(child))
            .executeUpdate();
        status(writer, child, operation.equals("resume") ? "PAUSED" : "PENDING");
      }
      status(writer, parent, operation.equals("resume") ? "PENDING" : "PAUSED");
      writer.getTransaction().commit();
      var failure =
          assertThrows(
              RatchetTransientStoreException.class, () -> change(readerStore, parent, operation));
      assertTrue(failure.getMessage().contains("snapshot predates batch " + parent));
      assertTrue(failure.getMessage().contains("retry in a new transaction"));
      reader.getTransaction().rollback();
      reader.getTransaction().begin();
      assertEquals(3, change(readerStore, parent, operation));
      reader.getTransaction().commit();
    } finally {
      for (var em : List.of(reader, writer)) {
        if (em.getTransaction().isActive()) em.getTransaction().rollback();
        em.close();
      }
    }
  }

  @Test
  void cancelMissingParentReturnsZero() {
    fixture.cleanupStore();
    emf = newFactory();
    var em = emf.createEntityManager();
    try {
      em.getTransaction().begin();
      assertEquals(0, store(em).cancelBatchChildren(UUID.randomUUID()));
      em.getTransaction().commit();
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
    }
  }

  private int change(MysqlJobStoreImpl store, UUID parent, String operation) {
    return switch (operation) {
      case "pause" -> store.pauseBatchChildren(parent);
      case "resume" -> store.resumeBatchChildren(parent);
      default -> store.cancelBatchChildren(parent);
    };
  }

  private void sweep(String operation, boolean staleSnapshot) {
    fixture.cleanupStore();
    emf = newFactory();
    var seed = emf.createEntityManager();
    var holder = emf.createEntityManager();
    var sweep = emf.createEntityManager();
    try {
      var seedStore = store(seed);
      seed.getTransaction().begin();
      UUID parent = seedStore.create(fixture.newPendingJob()).getId();
      UUID unrelated = seedStore.create(fixture.newPendingJob()).getId();
      UUID child = seedStore.create(fixture.newPendingJob()).getId();
      UUID running = seedStore.create(fixture.newPendingJob()).getId();
      UUID terminal = seedStore.create(fixture.newPendingJob()).getId();
      for (UUID id : List.of(child, running, terminal)) {
        seed.createNativeQuery(
                "UPDATE scheduler_job SET depends_on = ?, job_type = 'BATCH_CHILD' WHERE job_id ="
                    + " ?")
            .setParameter(1, UuidByteArrayConverter.toBytes(parent))
            .setParameter(2, UuidByteArrayConverter.toBytes(id))
            .executeUpdate();
      }
      status(seed, parent, operation.equals("resume") ? "PENDING" : "PAUSED");
      status(
          seed,
          child,
          staleSnapshot ? "RUNNING" : operation.equals("resume") ? "PAUSED" : "PENDING");
      status(seed, running, "RUNNING");
      seed.createNativeQuery("DELETE FROM scheduler_job_queue WHERE job_id = ?")
          .setParameter(1, UuidByteArrayConverter.toBytes(terminal))
          .executeUpdate();
      seed.createNativeQuery(
              "UPDATE scheduler_job SET terminal_status = 'SUCCEEDED' WHERE job_id = ?")
          .setParameter(1, UuidByteArrayConverter.toBytes(terminal))
          .executeUpdate();
      seed.getTransaction().commit();

      var sweepStore = store(sweep);
      sweep.getTransaction().begin();
      sweep.createNativeQuery("SET SESSION innodb_lock_wait_timeout = 1").executeUpdate();
      if (staleSnapshot) {
        sweep.createNativeQuery("SELECT COUNT(*) FROM scheduler_job").getSingleResult();
        holder.getTransaction().begin();
        status(holder, child, "PENDING");
        holder.getTransaction().commit();
      }
      holder.getTransaction().begin();
      holder
          .createNativeQuery(
              "SELECT job_id FROM scheduler_job_queue FORCE INDEX (PRIMARY) WHERE job_id = ? FOR"
                  + " UPDATE")
          .setParameter(1, UuidByteArrayConverter.toBytes(unrelated))
          .getSingleResult();
      int changed =
          switch (operation) {
            case "pause" -> sweepStore.pauseBatchChildren(parent);
            case "resume" -> sweepStore.resumeBatchChildren(parent);
            default -> sweepStore.cancelBatchChildren(parent);
          };
      assertEquals(1, changed);
      sweep.getTransaction().commit();
      holder.getTransaction().rollback();
      seed.getTransaction().begin();
      List<?> statuses =
          seed.createNativeQuery("SELECT status FROM scheduler_job_queue WHERE job_id = ?")
              .setParameter(1, UuidByteArrayConverter.toBytes(child))
              .getResultList();
      if (operation.equals("cancel")) assertTrue(statuses.isEmpty());
      else assertEquals(operation.equals("pause") ? "PAUSED" : "PENDING", statuses.get(0));
      assertEquals(
          "RUNNING",
          seed.createNativeQuery("SELECT status FROM scheduler_job_queue WHERE job_id = ?")
              .setParameter(1, UuidByteArrayConverter.toBytes(running))
              .getSingleResult());
      seed.getTransaction().commit();
    } finally {
      for (var em : List.of(sweep, holder, seed)) {
        if (em.getTransaction().isActive()) em.getTransaction().rollback();
        em.close();
      }
    }
  }

  @Test
  void discoveryAndLockingPlansUseDependencyAndPrimaryKeys() throws Exception {
    fixture.cleanupStore();
    emf = newFactory();
    var em = emf.createEntityManager();
    UUID parent;
    UUID child;
    try {
      var store = store(em);
      em.getTransaction().begin();
      parent = store.create(fixture.newPendingJob()).getId();
      child = store.create(fixture.newPendingJob()).getId();
      em.createNativeQuery(
              "UPDATE scheduler_job SET depends_on = ?, job_type = 'BATCH_CHILD' WHERE job_id = ?")
          .setParameter(1, UuidByteArrayConverter.toBytes(parent))
          .setParameter(2, UuidByteArrayConverter.toBytes(child))
          .executeUpdate();
      em.getTransaction().commit();
    } finally {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
      em.close();
    }
    var container = fixture.container();
    try (var connection =
        DriverManager.getConnection(
            container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      for (String sql :
          List.of(
              MysqlBatchOperations.CHILD_DISCOVERY_FIRST_SQL,
              MysqlBatchOperations.CHILD_DISCOVERY_SQL)) {
        try (var explain = connection.prepareStatement("EXPLAIN " + sql)) {
          explain.setBytes(1, UuidByteArrayConverter.toBytes(parent));
          if (sql.equals(MysqlBatchOperations.CHILD_DISCOVERY_SQL))
            explain.setBytes(2, new byte[16]);
          try (var rows = explain.executeQuery()) {
            assertTrue(rows.next());
            assertEquals("c", rows.getString("table"));
            assertEquals("idx_job_depends_on", rows.getString("key"));
            assertTrue(List.of("ref", "range").contains(rows.getString("type")));
            String extra = rows.getString("Extra");
            assertTrue(extra == null || !extra.contains("Using filesort"));
            assertFalse(rows.next());
          }
        }
      }
      try (var explain =
          connection.prepareStatement(
              "EXPLAIN " + MysqlBatchOperations.CHILD_LOCK_SQL.formatted("?", "'PENDING'"))) {
        explain.setBytes(1, UuidByteArrayConverter.toBytes(child));
        try (var rows = explain.executeQuery()) {
          assertTrue(rows.next());
          assertEquals("PRIMARY", rows.getString("key"));
        }
      }
    }
  }

  private void status(EntityManager em, UUID id, String status) {
    em.createNativeQuery("UPDATE scheduler_job_queue SET status = ? WHERE job_id = ?")
        .setParameter(1, status)
        .setParameter(2, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();
  }
}
