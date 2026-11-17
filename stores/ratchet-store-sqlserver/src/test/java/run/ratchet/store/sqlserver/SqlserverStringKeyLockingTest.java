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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.LockTimeoutException;
import jakarta.persistence.Persistence;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

/**
 * Checks that statements keyed on a VARCHAR column lock only the key they name. The driver binds
 * strings as NVARCHAR, and a scan in place of a seek would range-lock every key under HOLDLOCK or
 * make an UPDATE or DELETE wait on other keys' row locks. Each test holds a transaction open on one
 * key and checks that work on another key does not wait for it.
 */
class SqlserverStringKeyLockingTest {
  private final SqlserverTestFixture fixture = new SqlserverTestFixture();
  private EntityManagerFactory emf;
  private EntityManager seed;
  private EntityManager holder;
  private EntityManager other;

  @BeforeEach
  void open() {
    fixture.cleanupStore();
    emf = newFactory();
    seed = emf.createEntityManager();
    holder = emf.createEntityManager();
    other = emf.createEntityManager();
  }

  @AfterEach
  void close() {
    try {
      try {
        closeSession(other);
      } finally {
        try {
          closeSession(holder);
        } finally {
          closeSession(seed);
        }
      }
    } finally {
      try {
        if (emf != null) emf.close();
      } finally {
        fixture.cleanupStore();
      }
    }
  }

  private void closeSession(EntityManager em) {
    if (em == null) return;
    try {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
    } finally {
      em.close();
    }
  }

  private EntityManagerFactory newFactory() {
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    props.put("jakarta.persistence.jdbc.url", container.getJdbcUrl() + ";lockTimeout=1000");
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    props.put("hibernate.connection.pool_size", "4");
    props.put("hibernate.connection.isolation", "2");
    return Persistence.createEntityManagerFactory("ratchet-sqlserver-tck", props);
  }

  private SqlserverJobStoreImpl store(EntityManager em) {
    return store(em, RatchetOptions.defaults());
  }

  private SqlserverJobStoreImpl store(EntityManager em, RatchetOptions options) {
    var store = new SqlserverJobStoreImpl(() -> em, mock(MetricsCollector.class), options);
    store.checkIsolationLevel();
    return store;
  }

  private List<String> waited(Map<String, Consumer<SqlserverJobStoreImpl>> operations) {
    return waited(store(other), operations);
  }

  private List<String> waited(
      SqlserverJobStoreImpl store, Map<String, Consumer<SqlserverJobStoreImpl>> operations) {
    var result = new ArrayList<String>();
    for (var entry : operations.entrySet()) {
      try {
        other.getTransaction().begin();
        entry.getValue().accept(store);
        other.getTransaction().commit();
      } catch (RuntimeException e) {
        if (isLockTimeout(e)) {
          result.add(entry.getKey());
        } else {
          throw e;
        }
      } finally {
        if (other.getTransaction().isActive()) other.getTransaction().rollback();
      }
    }
    return result;
  }

  private static boolean isLockTimeout(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof LockTimeoutException
          || cause instanceof SQLException sql && sql.getErrorCode() == 1222) {
        return true;
      }
    }
    return false;
  }

  private static int rangeLocksHeldBy(EntityManager em) {
    return ((Number)
            em.createNativeQuery(
                    """
                    SELECT COUNT(*) FROM sys.dm_tran_locks
                    WHERE request_session_id = @@SPID AND resource_type = 'KEY'
                      AND request_mode LIKE 'Range%'
                    """)
                .getSingleResult())
        .intValue();
  }

  // Claims under the node that later acquires the permit: an acquire from another node is
  // SUPERSEDED.
  private JobEntity claimedJob(SqlserverJobStoreImpl store, String resource, String node) {
    var job = fixture.newPendingJob();
    job.setResourceName(resource);
    var created = store.create(job);
    var claimed = store.claimNextBatch(1, node);
    assertEquals(1, claimed.size());
    assertEquals(created.getId(), claimed.get(0).getId());
    return claimed.get(0);
  }

  private UUID waitingJob(SqlserverJobStoreImpl store, String signalKey) {
    var job = fixture.newPendingJob();
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey(signalKey);
    job.setSignalTimeout(Instant.now().plusSeconds(600));
    return store.create(job).getId();
  }

  private int deliver(SqlserverJobStoreImpl store, String key) {
    return store.deliverSignalByKey(
        key, null, null, "APPROVED", null, "test", Instant.now(), UUID.randomUUID().toString());
  }

  private void inSeed(Consumer<SqlserverJobStoreImpl> work) {
    try {
      seed.getTransaction().begin();
      work.accept(store(seed));
      seed.getTransaction().commit();
    } finally {
      if (seed.getTransaction().isActive()) seed.getTransaction().rollback();
    }
  }

  private void inSeedAllowingFailure(Consumer<SqlserverJobStoreImpl> work) {
    try {
      inSeed(work);
    } catch (RuntimeException ignored) {
      // A duplicate key is allowed when the cast name already exists.
    }
  }

  // SQL Server turns U+0101 into 'a' and U+4E2D into '?' when it casts to VARCHAR. A key with
  // either character must not match the ASCII key it casts to.
  @Test
  void permitAcquireAndConfigurationDoNotMatchALookAlikeResource() {
    JobEntity job;
    try {
      seed.getTransaction().begin();
      var seedStore = store(seed);
      seedStore.configureResource("res-a", 1, 0, null);
      job = claimedJob(seedStore, "res-a", "node-a");
      seed.getTransaction().commit();
    } finally {
      if (seed.getTransaction().isActive()) seed.getTransaction().rollback();
    }
    try {
      seed.getTransaction().begin();
      var seedStore = store(seed);
      assertThrows(
          ResourceNotConfiguredException.class,
          () -> seedStore.tryAcquirePermit("res-\u0101", job.getId(), job.getClaimSeq(), "node-a"));
    } finally {
      if (seed.getTransaction().isActive()) seed.getTransaction().rollback();
    }
    inSeedAllowingFailure(s -> s.configureResource("res-\u0101", 5, 0, "other"));
    inSeed(s -> assertEquals(1, s.findResourceLimit("res-a").orElseThrow().maxConcurrent()));
  }

  @Test
  void lockOperationsDoNotMatchALookAlikeLock() {
    inSeed(s -> assertTrue(s.tryLock("lock-a", Duration.ofHours(1), "node-a")));
    inSeed(s -> assertFalse(s.renewLock("lock-\u0101", Duration.ofHours(1), "node-a")));
    inSeed(s -> s.unlock("lock-\u0101", "node-a"));
    inSeedAllowingFailure(
        s -> assertFalse(s.tryLock("lock-\u0101", Duration.ofHours(1), "node-a")));
    inSeed(s -> assertFalse(s.tryLock("lock-a", Duration.ofHours(1), "node-b")));
  }

  @Test
  void nodeOperationsDoNotMatchALookAlikeNode() {
    Instant t0 = Instant.now().minusSeconds(3600).truncatedTo(ChronoUnit.SECONDS);
    inSeed(s -> s.upsertHeartbeat("node-a", t0));
    inSeed(s -> assertEquals(0, s.deleteInactiveNodesByIds(List.of("node-\u0101"))));
    inSeedAllowingFailure(s -> s.upsertHeartbeat("node-\u0101", Instant.now()));
    inSeed(s -> assertEquals(t0, s.findNodeById("node-a").orElseThrow().getLastHeartbeat()));
  }

  @Test
  void deleteInactiveNodesByIdsTakesMoreIdsThanTheParameterLimit() {
    var ids = new ArrayList<String>();
    for (int i = 0; i < 2200; i++) {
      ids.add(String.format("bulk-%04d", i));
    }
    Instant t0 = Instant.now().minusSeconds(3600).truncatedTo(ChronoUnit.SECONDS);
    inSeed(
        s -> {
          s.upsertHeartbeat(ids.get(5), t0);
          s.upsertHeartbeat(ids.get(2150), t0);
          s.upsertHeartbeat(ids.get(2190), t0);
          seed.createNativeQuery(
                  """
                  INSERT INTO scheduler_business_key_reservation
                    (business_key, owner_job_id, owner_table, held_until, held_by_node, held_claim_seq)
                  VALUES ('bulk-hold-key', ?1, 'QUEUE', ?2, ?3, 1)
                  """)
              .setParameter(1, UuidByteArrayConverter.toBytes(UUID.randomUUID()))
              .setParameter(2, Timestamp.from(Instant.now().plusSeconds(3600)))
              .setParameter(3, ids.get(2190))
              .executeUpdate();
        });
    inSeed(s -> assertEquals(2, s.deleteInactiveNodesByIds(ids)));
    inSeed(
        s -> {
          assertTrue(s.findNodeById(ids.get(5)).isEmpty());
          assertTrue(s.findNodeById(ids.get(2150)).isEmpty());
          assertTrue(s.findNodeById(ids.get(2190)).isPresent());
        });
  }

  @Test
  void signalDeliveryDoesNotMatchALookAlikeKey() {
    inSeed(
        s -> {
          waitingJob(s, "sig-a");
          waitingJob(s, "sig-?");
        });
    inSeed(s -> assertEquals(0, deliver(s, "sig-\u0101")));
    inSeed(s -> assertEquals(0, deliver(s, "sig-\u4E2D")));
    inSeed(s -> assertEquals(1, deliver(s, "sig-a")));
    inSeed(s -> assertEquals(1, deliver(s, "sig-?")));
  }

  @Test
  void permitAcquireAndWakeDoNotWaitForAnAcquireOnAnotherResource() {
    seed.getTransaction().begin();
    var seedStore = store(seed);
    seedStore.configureResource("res-a", 1, 0, null);
    seedStore.configureResource("res-b", 1, 0, null);
    seedStore.configureResource("res-c", 1, 0, null);
    var jobA = claimedJob(seedStore, "res-a", "node-a");
    var jobB = claimedJob(seedStore, "res-b", "node-b");
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    assertEquals(
        ACQUIRED,
        store(holder).tryAcquirePermit("res-a", jobA.getId(), jobA.getClaimSeq(), "node-a"));
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put(
        "tryAcquirePermit on res-b",
        s ->
            assertEquals(
                ACQUIRED, s.tryAcquirePermit("res-b", jobB.getId(), jobB.getClaimSeq(), "node-b")));
    operations.put("wakeWaiters on res-b", s -> s.wakeWaiters("res-b"));
    assertEquals(
        List.of(), waited(operations), "operations that waited for the open acquire on res-a");
    assertEquals(0, rangeLocksHeldBy(holder), "the acquire on res-a range-locked other resources");
  }

  @Test
  void permitAcquireDoesNotWaitForAWakeOnAnotherResource() {
    seed.getTransaction().begin();
    var seedStore = store(seed);
    seedStore.configureResource("res-a", 1, 0, null);
    seedStore.configureResource("res-b", 1, 0, null);
    seedStore.configureResource("res-c", 1, 0, null);
    var jobB = claimedJob(seedStore, "res-b", "node-b");
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    store(holder).wakeWaiters("res-a");
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put(
        "tryAcquirePermit on res-b",
        s ->
            assertEquals(
                ACQUIRED, s.tryAcquirePermit("res-b", jobB.getId(), jobB.getClaimSeq(), "node-b")));
    assertEquals(
        List.of(), waited(operations), "operations that waited for the open wake-up on res-a");
    assertEquals(0, rangeLocksHeldBy(holder), "the wake-up on res-a range-locked other resources");
  }

  @Test
  void resourceConfigurationDoesNotBlockAcquiresOnOtherResources() {
    seed.getTransaction().begin();
    var seedStore = store(seed);
    seedStore.configureResource("res-a", 1, 0, null);
    seedStore.configureResource("res-b", 1, 0, null);
    seedStore.configureResource("res-c", 1, 0, null);
    var jobB = claimedJob(seedStore, "res-b", "node-b");
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    store(holder).configureResource("res-a", 2, 0, "changed");
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put(
        "tryAcquirePermit on res-b",
        s ->
            assertEquals(
                ACQUIRED, s.tryAcquirePermit("res-b", jobB.getId(), jobB.getClaimSeq(), "node-b")));
    assertEquals(
        List.of(),
        waited(operations),
        "operations that waited for the open configuration of res-a");
    assertEquals(0, rangeLocksHeldBy(holder), "configuring res-a range-locked other resources");
  }

  @Test
  void lockOperationsDoNotWaitForAnotherLocksOpenTransaction() {
    seed.getTransaction().begin();
    var seedStore = store(seed);
    assertTrue(seedStore.tryLock("lock-a", Duration.ofHours(1), "node-a"));
    assertTrue(seedStore.tryLock("lock-b", Duration.ofHours(1), "node-b"));
    assertTrue(seedStore.tryLock("lock-c", Duration.ofHours(1), "node-c"));
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    assertTrue(store(holder).tryLock("lock-b", Duration.ofHours(1), "node-b"));
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put(
        "tryLock lock-a", s -> assertTrue(s.tryLock("lock-a", Duration.ofHours(1), "node-a")));
    operations.put(
        "renewLock lock-a", s -> assertTrue(s.renewLock("lock-a", Duration.ofHours(1), "node-a")));
    operations.put("unlock lock-a", s -> s.unlock("lock-a", "node-a"));
    assertEquals(
        List.of(),
        waited(operations),
        "lock operations that waited for the open tryLock on lock-b");
    assertEquals(0, rangeLocksHeldBy(holder), "tryLock on lock-b range-locked other locks");
  }

  @Test
  void heartbeatAndNodeDeleteDoNotWaitForAnotherNodesHeartbeat() {
    Instant now = Instant.now();
    seed.getTransaction().begin();
    var seedStore = store(seed);
    seedStore.upsertHeartbeat("node-a", now);
    seedStore.upsertHeartbeat("node-b", now);
    seedStore.upsertHeartbeat("node-c", now);
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    store(holder).upsertHeartbeat("node-b", now);
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put("upsertHeartbeat node-a", s -> s.upsertHeartbeat("node-a", now));
    operations.put(
        "deleteInactiveNodesByIds node-c",
        s -> assertEquals(1, s.deleteInactiveNodesByIds(List.of("node-c"))));
    assertEquals(
        List.of(),
        waited(operations),
        "node operations that waited for the open heartbeat of node-b");
    assertEquals(0, rangeLocksHeldBy(holder), "the heartbeat of node-b range-locked other nodes");
  }

  @Test
  void signalDeliveryByKeyDoesNotWaitForADeliveryToAnotherKey() {
    seed.getTransaction().begin();
    var seedStore = store(seed);
    waitingJob(seedStore, "sig-a");
    waitingJob(seedStore, "sig-b");
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    assertEquals(1, deliver(store(holder), "sig-b"));
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put("deliverSignalByKey sig-a", s -> assertEquals(1, deliver(s, "sig-a")));
    assertEquals(
        List.of(),
        waited(operations),
        "signal deliveries that waited for the open delivery to sig-b");
  }

  @Test
  void unboostedClaimLocksOnlyTheRowsItClaims() {
    // Priority boosting off makes the claim order match idx_claim_pending_priority.
    RatchetOptions options =
        RatchetOptions.builder().store(s -> s.priorityBoostIntervalMinutes(0)).build();
    seed.getTransaction().begin();
    var seedStore = store(seed);
    var ids = new ArrayList<UUID>();
    for (int i = 0; i < 3; i++) {
      ids.add(seedStore.create(fixture.newPendingJob()).getId());
    }
    seed.getTransaction().commit();

    holder.getTransaction().begin();
    var claimed =
        store(holder, options).claimNextBatchOptimized(JobExecutionType.SINGLE, 1, "node-a");
    assertEquals(1, claimed.size());
    UUID taken = claimed.get(0).id();
    UUID untouched = ids.stream().filter(id -> !id.equals(taken)).findFirst().orElseThrow();
    var otherStore = store(other, options);
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put(
        "cancelJob of a job the claim did not take", s -> assertTrue(s.cancelJob(untouched)));
    assertEquals(
        List.of(),
        waited(otherStore, operations),
        "operations that waited for jobs the open claim did not take");

    other.getTransaction().begin();
    int second = otherStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 5, "node-b").size();
    other.getTransaction().commit();
    assertEquals(1, second, "a second claim found the job the open claim did not take");
  }
}
