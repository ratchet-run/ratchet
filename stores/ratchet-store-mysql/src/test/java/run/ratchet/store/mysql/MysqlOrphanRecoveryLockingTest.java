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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

class MysqlOrphanRecoveryLockingTest {
  private final MysqlTestFixture fixture = new MysqlTestFixture();
  private EntityManagerFactory emf;

  private EntityManagerFactory newFactory(int isolation) {
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    props.put("jakarta.persistence.jdbc.url", container.getJdbcUrl());
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    props.put("hibernate.connection.isolation", Integer.toString(isolation));
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
  @ValueSource(ints = {4, 2})
  void periodicScanDoesNotBlockOnHealthyRunningRows(int isolation) {
    scanWithOtherOwnersRowLocked(isolation, false);
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 2})
  void startupScanDoesNotBlockOnOtherNodesRunningRows(int isolation) {
    scanWithOtherOwnersRowLocked(isolation, true);
  }

  private void scanWithOtherOwnersRowLocked(int isolation, boolean startup) {
    fixture.cleanupStore();
    emf = newFactory(isolation);
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
      holder
          .createNativeQuery("SELECT job_id FROM scheduler_job_queue WHERE job_id = ? FOR UPDATE")
          .setParameter(1, UuidByteArrayConverter.toBytes(healthyId))
          .getSingleResult();
      var scanStore = store(scan);
      scan.getTransaction().begin();
      scan.createNativeQuery("SET SESSION innodb_lock_wait_timeout = 1").executeUpdate();
      // Before this fix the scan waited on the held row and failed with a lock-wait timeout.
      var recovery =
          startup
              ? scanStore.resetOrphanJobsForNode("dead", 3, 100)
              : scanStore.resetOrphanJobsBefore(cutoff, 3, 100);
      assertEquals(1, recovery.reset());
      scan.getTransaction().commit();

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
      for (var em : List.of(scan, holder, seed)) {
        if (em.getTransaction().isActive()) em.getTransaction().rollback();
        em.close();
      }
    }
  }

  private UUID runningJob(MysqlJobStoreImpl store, String owner, Instant pickedAt) {
    var job = store.create(fixture.newPendingJob());
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy(owner);
    job.setPickedAt(pickedAt);
    store.save(job);
    return job.getId();
  }
}
