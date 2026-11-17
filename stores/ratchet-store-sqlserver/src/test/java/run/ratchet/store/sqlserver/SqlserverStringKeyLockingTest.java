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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.hibernate.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

  // Which plan SQL Server picks for a joined lock read depends on the row count and the
  // statistics, so each case clears the plan cache first.
  @ParameterizedTest(name = "{0} jobs, statistics updated: {1}")
  @CsvSource({"2, true", "2, false", "3, true", "3, false", "50, true", "50, false"})
  void tagCancelDoesNotWaitForAnOpenCancelOfAnotherTag(int jobs, boolean updateStatistics) {
    inSeed(
        s -> {
          for (int n = 1; n <= jobs; n++) {
            var job = fixture.newPendingJob("tag-" + n);
            job.setBusinessKey("tag-key-" + n);
            s.create(job);
          }
        });
    if (updateStatistics) {
      runAdmin(
          "UPDATE STATISTICS scheduler_job_queue WITH FULLSCAN",
          "UPDATE STATISTICS scheduler_job WITH FULLSCAN",
          "UPDATE STATISTICS scheduler_job_tag WITH FULLSCAN",
          "UPDATE STATISTICS scheduler_business_key_reservation WITH FULLSCAN");
    }
    runAdmin("ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE");

    holder.getTransaction().begin();
    assertEquals(1, store(holder).cancelJobsByTag("tag-1"));
    // The cancel deletes the rows it locked, which turns their U locks into X locks. A U lock
    // left over is on a row the cancel read but did not cancel.
    assertEquals(
        List.of(),
        updateLocksHeldBy(holder),
        "indexes whose keys the open cancel of tag-1 U-locked without cancelling them");
    var operations = new LinkedHashMap<String, Consumer<SqlserverJobStoreImpl>>();
    operations.put("cancelJobsByTag tag-2", s -> assertEquals(1, s.cancelJobsByTag("tag-2")));
    assertEquals(
        List.of(), waited(operations), "tag cancels that waited for the open cancel of tag-1");
  }

  @Test
  void tagCancelTakesMoreJobsThanTheParameterLimit() {
    inSeed(
        s -> {
          for (int i = 0; i < 2200; i++) {
            var job = fixture.newPendingJob("bulk-tag");
            if (i % 500 == 0) job.setBusinessKey("bulk-key-" + i);
            s.create(job);
          }
          s.create(fixture.newPendingJob("other-tag"));
        });
    inSeed(s -> assertEquals(2200, s.cancelJobsByTag("bulk-tag")));
    String taggedQueueRows =
        """
        SELECT COUNT(*) FROM scheduler_job_queue q
        JOIN scheduler_job_tag t ON t.job_id = q.job_id WHERE t.tag = ?
        """;
    assertEquals(0, count(taggedQueueRows, "bulk-tag"), "queue rows left for the tag");
    assertEquals(
        2200,
        count(
            """
            SELECT COUNT(*) FROM scheduler_job j
            JOIN scheduler_job_tag t ON t.job_id = j.job_id
            WHERE t.tag = ? AND j.terminal_status = 'CANCELED'
            """,
            "bulk-tag"),
        "cancelled jobs");
    assertEquals(1, count(taggedQueueRows, "other-tag"), "queue rows of another tag");
    assertEquals(
        0,
        count(
            "SELECT COUNT(*) FROM scheduler_business_key_reservation WHERE business_key LIKE ?",
            "bulk-key-%"),
        "reservations of the cancelled jobs");
  }

  // Guards the order that the other multi-row queue writes use, across lock chunks, and checks
  // that the cancel locks every chunk before it writes a job row.
  @Test
  void tagCancelLocksQueueRowsInJobIdByteOrder() throws Exception {
    var ids = new ArrayList<UUID>();
    inSeed(
        s -> {
          for (int i = 0; i < 600; i++) {
            ids.add(s.create(fixture.newPendingJob("order-tag")).getId());
          }
        });
    ids.sort(Comparator.comparing(UUID::toString));
    UUID blocked = ids.get(550);

    other.getTransaction().begin();
    other
        .createNativeQuery(
            "SELECT job_id FROM scheduler_job_queue WITH (UPDLOCK, ROWLOCK) WHERE job_id = ?1")
        .setParameter(1, UuidByteArrayConverter.toBytes(blocked))
        .getResultList();

    holder.getTransaction().begin();
    // A prepared statement runs in sp_executesql scope, which undoes SET when it returns, so set
    // the session's lock timeout through a plain statement.
    holder
        .unwrap(Session.class)
        .doWork(
            connection -> {
              try (Statement statement = connection.createStatement()) {
                statement.execute("SET LOCK_TIMEOUT 30000");
              }
            });
    int holderSession =
        ((Number) holder.createNativeQuery("SELECT @@SPID").getSingleResult()).intValue();
    var holderStore = store(holder);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection observer = fixture.openConnection()) {
      Future<Integer> cancel = executor.submit(() -> holderStore.cancelJobsByTag("order-tag"));
      Map<String, UUID> byLockResource = queueLockResources(observer);
      List<UUID> waitingOn = List.of();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (waitingOn.isEmpty() && System.nanoTime() < deadline) {
        waitingOn = queueKeysLockedBy(observer, holderSession, "WAIT", byLockResource);
        if (waitingOn.isEmpty()) Thread.sleep(20);
      }
      assertEquals(List.of(blocked), waitingOn, "the queue row the cancel waits on");
      var granted =
          new HashSet<>(queueKeysLockedBy(observer, holderSession, "GRANT", byLockResource));
      assertEquals(
          new HashSet<>(ids.subList(0, 550)),
          granted,
          "queue rows the cancel locked before it reached the blocked row");
      assertEquals(
          0, jobKeyLocksHeldBy(observer, holderSession), "job rows written before every lock");
      other.getTransaction().rollback();
      assertEquals(600, cancel.get(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  private void runAdmin(String... statements) {
    try (Connection connection = fixture.openConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        statement.execute(sql);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private int count(String sql, String parameter) {
    try (Connection connection = fixture.openConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, parameter);
      try (ResultSet rows = statement.executeQuery()) {
        rows.next();
        return rows.getInt(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  // Lists "index: count" for the U key locks the session holds.
  private static List<String> updateLocksHeldBy(EntityManager em) {
    @SuppressWarnings("unchecked")
    List<Object> indexes =
        em.createNativeQuery(
                """
                SELECT i.name + ': ' + CAST(COUNT(*) AS VARCHAR(10))
                FROM sys.dm_tran_locks l
                JOIN sys.partitions p ON p.hobt_id = l.resource_associated_entity_id
                JOIN sys.indexes i ON i.object_id = p.object_id AND i.index_id = p.index_id
                WHERE l.request_session_id = @@SPID AND l.resource_type = 'KEY'
                  AND l.request_mode = 'U'
                GROUP BY i.name
                ORDER BY i.name
                """)
            .getResultList();
    return indexes.stream().map(String::valueOf).toList();
  }

  // Maps each queue row's primary-key lock resource to its job id.
  private static Map<String, UUID> queueLockResources(Connection connection) throws SQLException {
    var resources = new HashMap<String, UUID>();
    try (Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                """
                SELECT %%lockres%%, job_id
                FROM scheduler_job_queue WITH (NOLOCK, INDEX(pk_scheduler_job_queue))
                """)) {
      while (rows.next()) {
        resources.put(
            rows.getString(1).strip(), UuidByteArrayConverter.fromBytes(rows.getBytes(2)));
      }
    }
    return resources;
  }

  private static List<UUID> queueKeysLockedBy(
      Connection connection, int session, String status, Map<String, UUID> byLockResource)
      throws SQLException {
    var ids = new ArrayList<UUID>();
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
            SELECT l.resource_description
            FROM sys.dm_tran_locks l
            JOIN sys.partitions p ON p.hobt_id = l.resource_associated_entity_id
            JOIN sys.indexes i ON i.object_id = p.object_id AND i.index_id = p.index_id
            WHERE l.request_session_id = ? AND l.resource_type = 'KEY'
              AND l.request_status = ? AND i.name = 'pk_scheduler_job_queue'
            """)) {
      statement.setInt(1, session);
      statement.setString(2, status);
      try (ResultSet rows = statement.executeQuery()) {
        while (rows.next()) {
          String resource = rows.getString(1).strip();
          UUID id = byLockResource.get(resource);
          if (id == null) {
            throw new IllegalStateException("no queue row for lock resource " + resource);
          }
          ids.add(id);
        }
      }
    }
    return ids;
  }

  private static int jobKeyLocksHeldBy(Connection connection, int session) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
            SELECT COUNT(*)
            FROM sys.dm_tran_locks l
            JOIN sys.partitions p ON p.hobt_id = l.resource_associated_entity_id
            WHERE l.request_session_id = ? AND l.resource_type = 'KEY'
              AND p.object_id = OBJECT_ID('scheduler_job')
            """)) {
      statement.setInt(1, session);
      try (ResultSet rows = statement.executeQuery()) {
        rows.next();
        return rows.getInt(1);
      }
    }
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
