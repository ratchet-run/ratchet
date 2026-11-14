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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.LockTimeoutException;
import jakarta.persistence.Persistence;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverOrphanRecoveryLockingTest {
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

  @Test
  void periodicScanDoesNotBlockOnHealthyRunningRows() {
    scanWithOtherOwnersRowLocked(false, false);
  }

  @Test
  void startupScanDoesNotBlockOnOtherNodesRunningRows() {
    scanWithOtherOwnersRowLocked(true, false);
  }

  @Test
  void periodicScanDoesNotBlockOnExclusivelyLockedHealthyRows() {
    scanWithOtherOwnersRowLocked(false, true);
  }

  @Test
  void startupScanDoesNotBlockOnExclusivelyLockedRows() {
    scanWithOtherOwnersRowLocked(true, true);
  }

  @Test
  void staleOwnerHeartbeatWaitsForTheScanToCommit() {
    fixture.cleanupStore();
    emf = newFactory();
    Instant now = Instant.now();
    Instant cutoff = now.minusSeconds(60);
    var seed = emf.createEntityManager();
    var heartbeat = emf.createEntityManager();
    var scan = emf.createEntityManager();
    try {
      var seedStore = store(seed);
      seed.getTransaction().begin();
      seedStore.upsertHeartbeat("live", now);
      seedStore.upsertHeartbeat("late", now.minusSeconds(3600));
      UUID jobId = runningJob(seedStore, "late", cutoff.minusSeconds(60));
      seed.getTransaction().commit();

      var scanStore = store(scan);
      scan.getTransaction().begin();
      var recovery = scanStore.resetOrphanJobsBefore(cutoff, 3, 500);
      assertEquals(1, recovery.reset());

      var heartbeatStore = store(heartbeat);
      heartbeat.getTransaction().begin();
      heartbeatStore.upsertHeartbeat("live", now);
      heartbeat.getTransaction().commit();

      heartbeat.getTransaction().begin();
      // Only the candidate owner's node row stays shared-locked until the scan commits. The
      // connection's lockTimeout makes SQL Server end the wait with error 1222, which surfaces as
      // a lock timeout rather than a transient store error.
      assertThrows(LockTimeoutException.class, () -> heartbeatStore.upsertHeartbeat("late", now));
      if (heartbeat.getTransaction().isActive()) heartbeat.getTransaction().rollback();

      scan.getTransaction().commit();

      heartbeat.getTransaction().begin();
      heartbeatStore.upsertHeartbeat("late", now);
      heartbeat.getTransaction().commit();

      scan.getTransaction().begin();
      var job = scanStore.findById(jobId).orElseThrow();
      assertEquals(JobStatus.PENDING, job.getStatus());
      assertNull(job.getPickedBy());
      scan.getTransaction().commit();
    } finally {
      closeSessions(List.of(scan, heartbeat, seed));
    }
  }

  @Test
  void periodicScanRecoversOrphansPageByPage() {
    fixture.cleanupStore();
    emf = newFactory();
    Instant cutoff = Instant.now().minusSeconds(60);
    int count = 501;
    var seed = emf.createEntityManager();
    var scan = emf.createEntityManager();
    try {
      var seedStore = store(seed);
      seed.getTransaction().begin();
      seedStore.upsertHeartbeat("dead", cutoff.minusSeconds(3600));
      for (int i = 0; i < count; i++) {
        runningJob(seedStore, "dead", cutoff.minusSeconds(60));
      }
      seed.getTransaction().commit();

      // Each page commits on its own, and another session sees its result before the next page.
      var scanStore = store(scan);
      scan.getTransaction().begin();
      assertEquals(500, scanStore.resetOrphanJobsBefore(cutoff, 3, 500).reset());
      scan.getTransaction().commit();
      assertEquals(count - 500, runningCount(seed, "dead"));

      scan.getTransaction().begin();
      assertEquals(count - 500, scanStore.resetOrphanJobsBefore(cutoff, 3, 500).reset());
      scan.getTransaction().commit();
      assertEquals(0, runningCount(seed, "dead"));
    } finally {
      closeSessions(List.of(scan, seed));
    }
  }

  private static int runningCount(EntityManager em, String owner) {
    em.getTransaction().begin();
    try {
      return ((Number)
              em.createNativeQuery(
                      """
                      SELECT COUNT(*) FROM scheduler_job_queue
                      WHERE picked_by = ? AND status = 'RUNNING'
                      """)
                  .setParameter(1, owner)
                  .getSingleResult())
          .intValue();
    } finally {
      em.getTransaction().commit();
    }
  }

  private void scanWithOtherOwnersRowLocked(boolean startup, boolean exclusive) {
    fixture.cleanupStore();
    emf = newFactory();
    Instant now = Instant.now();
    Instant cutoff = now.minusSeconds(60);
    var seed = emf.createEntityManager();
    var holder = emf.createEntityManager();
    var scan = emf.createEntityManager();
    try {
      var seedStore = store(seed);
      seed.getTransaction().begin();
      seedStore.upsertHeartbeat("live", now);
      seedStore.upsertHeartbeat("dead", now.minusSeconds(3600));
      UUID healthyId = runningJob(seedStore, "live", cutoff.minusSeconds(60));
      UUID orphanId = runningJob(seedStore, "dead", cutoff.minusSeconds(60));
      seed.getTransaction().commit();

      holder.getTransaction().begin();
      if (exclusive) {
        holder
            .createNativeQuery(
                """
                UPDATE scheduler_job_queue SET updated_at = SYSUTCDATETIME()
                WHERE job_id = ?
                """)
            .setParameter(1, UuidByteArrayConverter.toBytes(healthyId))
            .executeUpdate();
      } else {
        holder
            .createNativeQuery(
                """
                SELECT job_id FROM scheduler_job_queue WITH (UPDLOCK, ROWLOCK)
                WHERE job_id = ?
                """)
            .setParameter(1, UuidByteArrayConverter.toBytes(healthyId))
            .getSingleResult();
      }
      var scanStore = store(scan);
      scan.getTransaction().begin();
      // Before this fix the scan waited on the locked healthy row until the lock timeout.
      var recovery =
          startup
              ? scanStore.resetOrphanJobsForNode("dead", 3, 500)
              : scanStore.resetOrphanJobsBefore(cutoff, 3, 500);
      assertEquals(1, recovery.reset());
      scan.getTransaction().commit();
      if (exclusive) holder.getTransaction().rollback();

      scan.getTransaction().begin();
      var orphan = scanStore.findById(orphanId).orElseThrow();
      assertEquals(JobStatus.PENDING, orphan.getStatus());
      assertNull(orphan.getPickedBy());
      assertEquals(
          1,
          ((Number)
                  scan.createNativeQuery(
                          "SELECT crash_count FROM scheduler_job_queue WHERE job_id = ?")
                      .setParameter(1, UuidByteArrayConverter.toBytes(orphanId))
                      .getSingleResult())
              .intValue());
      var healthy = scanStore.findById(healthyId).orElseThrow();
      assertEquals(JobStatus.RUNNING, healthy.getStatus());
      assertEquals("live", healthy.getPickedBy());
      scan.getTransaction().commit();
    } finally {
      closeSessions(List.of(scan, holder, seed));
    }
  }

  private void closeSessions(List<EntityManager> sessions) {
    if (sessions.isEmpty()) return;
    EntityManager em = sessions.get(0);
    try {
      try {
        if (em.getTransaction().isActive()) em.getTransaction().rollback();
      } finally {
        em.close();
      }
    } finally {
      closeSessions(sessions.subList(1, sessions.size()));
    }
  }

  private UUID runningJob(SqlserverJobStoreImpl store, String owner, Instant pickedAt) {
    var job = store.create(fixture.newPendingJob());
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy(owner);
    job.setPickedAt(pickedAt);
    store.save(job);
    return job.getId();
  }
}
