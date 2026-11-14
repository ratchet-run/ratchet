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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverBatchSweepLockingTest {
  private final SqlserverTestFixture fixture = new SqlserverTestFixture();
  private EntityManagerFactory emf;

  private EntityManagerFactory newFactory() {
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    // A SET LOCK_TIMEOUT sent as a prepared statement only lasts for that sp_executesql call, so
    // the driver's lockTimeout property sets it for the whole session instead.
    props.put("jakarta.persistence.jdbc.url", container.getJdbcUrl() + ";lockTimeout=1000");
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    props.put("hibernate.connection.pool_size", "4");
    props.put("hibernate.connection.isolation", "2");
    return Persistence.createEntityManagerFactory("ratchet-sqlserver-tck", props);
  }

  private SqlserverJobStoreImpl store(EntityManager em) {
    var store =
        new SqlserverJobStoreImpl(
            () -> em, mock(MetricsCollector.class), RatchetOptions.defaults());
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
  void claimDoesNotWaitForJobPausedInAnotherTransaction() {
    fixture.cleanupStore();
    emf = newFactory();
    var holder = emf.createEntityManager();
    var claimer = emf.createEntityManager();
    try {
      holder.getTransaction().begin();
      UUID paused = store(holder).create(fixture.newPendingJob()).getId();
      var others =
          List.of(
              store(holder).create(fixture.newPendingJob()).getId(),
              store(holder).create(fixture.newPendingJob()).getId(),
              store(holder).create(fixture.newPendingJob()).getId());
      holder.getTransaction().commit();
      holder.getTransaction().begin();
      assertTrue(store(holder).transitionToPaused(paused, JobStatus.PENDING));
      claimer.getTransaction().begin();
      var claimed =
          store(claimer).claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "claim-node");
      claimer.getTransaction().commit();
      holder.getTransaction().rollback();
      assertEquals(
          others.stream().collect(Collectors.toSet()),
          claimed.stream().map(JobClaimDto::id).collect(Collectors.toSet()));
    } finally {
      for (var em : List.of(claimer, holder)) {
        if (em.getTransaction().isActive()) em.getTransaction().rollback();
        em.close();
      }
    }
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
      if (staleSnapshot) {
        sweep.createNativeQuery("SELECT COUNT(*) FROM scheduler_job").getSingleResult();
        holder.getTransaction().begin();
        status(holder, child, "PENDING");
        holder.getTransaction().commit();
      }
      holder.getTransaction().begin();
      holder
          .createNativeQuery(
              "SELECT job_id FROM scheduler_job_queue WITH (UPDLOCK, ROWLOCK, FORCESEEK"
                  + " (pk_scheduler_job_queue (job_id))) WHERE job_id = ?")
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

  private void status(EntityManager em, UUID id, String status) {
    em.createNativeQuery("UPDATE scheduler_job_queue SET status = ? WHERE job_id = ?")
        .setParameter(1, status)
        .setParameter(2, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();
  }
}
