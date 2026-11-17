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
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.LockTimeoutException;
import jakarta.persistence.Persistence;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverBoostedClaimLockingTest {
  private static final RatchetOptions OPTIONS =
      RatchetOptions.builder().store(s -> s.priorityBoostIntervalMinutes(15)).build();
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
    return store(em, OPTIONS);
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void typedClaimLocksOnlyReturnedRowsWithEitherStatistics(boolean fullscan) {
    seedBacklog(1000, true, false);
    preparePlan(fullscan);
    checkClaims(false, NodeTagFilter.NONE, ExecutionTargetFilter.any());
  }

  @Test
  void hydratedClaimLocksOnlyReturnedRows() {
    seedBacklog(1000, true, false);
    checkClaims(true, NodeTagFilter.NONE, ExecutionTargetFilter.any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void refillsPastHeldRowsInOneOrSeveralStreams(boolean mixed) {
    seedBacklog(40, false, mixed);
    checkClaims(false, NodeTagFilter.NONE, ExecutionTargetFilter.any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filtersBeforeLocking(boolean targetFilter) {
    seed.getTransaction().begin();
    var store = store(seed);
    Instant now = Instant.now().minusSeconds(60);
    for (int i = 0; i < 60; i++) {
      var job = fixture.newPendingJob();
      job.setPriority(i < 40 ? JobPriority.CRITICAL : JobPriority.LOWEST);
      job.setScheduledTime(now.minusSeconds(i < 40 ? 7200 : 0));
      job.setExecutionTarget(i < 40 ? "wrong" : "wanted");
      job.setTags(List.of(i < 40 ? "wrong" : "wanted"));
      store.create(job);
    }
    seed.getTransaction().commit();
    checkClaims(
        false,
        targetFilter ? NodeTagFilter.NONE : new NodeTagFilter(List.of("wanted"), List.of()),
        targetFilter
            ? ExecutionTargetFilter.matching(List.of("wanted"), false)
            : ExecutionTargetFilter.any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void serialClaimsFollowSqlBoostedOrderIncludingUnsignedIds(boolean hydrated) {
    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    seed.getTransaction().begin();
    var store = store(seed);
    List<UUID> ids =
        List.of(
            UUID.fromString("7f000000-0000-0000-0000-000000000000"),
            UUID.fromString("80000000-0000-0000-0000-000000000000"),
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID());
    // LOWEST aged 127 minutes beats fresh CRITICAL; the first pair ties on score and time.
    int[] priorities = {0, 0, 4, 1, 0};
    int[] minutes = {127, 127, 7, 112, 112};
    for (int i = 0; i < ids.size(); i++) {
      var job = fixture.newPendingJob();
      job.setId(ids.get(i));
      job.setPriority(JobPriority.values()[priorities[i]]);
      job.setScheduledTime(now.minusSeconds(minutes[i] * 60L));
      store.create(job);
    }
    seed.getTransaction().commit();
    seed.getTransaction().begin();
    @SuppressWarnings("unchecked")
    List<byte[]> sqlIds =
        seed.createNativeQuery(
                """
                SELECT job_id FROM scheduler_job_queue
                WHERE status = 'PENDING' AND scheduled_time <= ?
                ORDER BY (priority + FLOOR(GREATEST(0,
                  DATEDIFF(SECOND, scheduled_time, ?)/60.0) / ?)) DESC,
                  scheduled_time ASC, job_id ASC
                """)
            .setParameter(1, Timestamp.from(now))
            .setParameter(2, Timestamp.from(now))
            .setParameter(3, 15)
            .getResultList();
    List<UUID> expected = sqlIds.stream().map(UuidByteArrayConverter::fromBytes).toList();
    seed.getTransaction().commit();
    assertEquals(ids.subList(0, 2), expected.subList(0, 2));
    List<UUID> actual = new ArrayList<>();
    for (int attempt = 0; attempt < ids.size() && actual.size() < ids.size(); attempt++) {
      holder.getTransaction().begin();
      actual.addAll(claim(holder, hydrated, 2, NodeTagFilter.NONE, ExecutionTargetFilter.any()));
      holder.getTransaction().commit();
    }
    assertEquals(expected, actual);
  }

  private void seedBacklog(int count, boolean twoTypes, boolean mixed) {
    seed.getTransaction().begin();
    var store = store(seed);
    Instant now = Instant.now().minusSeconds(60);
    for (int i = 0; i < count; i++) {
      var job = fixture.newPendingJob();
      job.setJobType(
          twoTypes && i % 2 == 1 ? JobExecutionType.CHAIN_STEP : JobExecutionType.SINGLE);
      job.setPriority(JobPriority.values()[twoTypes || mixed ? i % 5 : 0]);
      job.setScheduledTime(now.minusSeconds((twoTypes ? i % 240 : i) * 60L));
      store.create(job);
    }
    seed.getTransaction().commit();
  }

  // SQL Server refuses ALTER DATABASE SCOPED CONFIGURATION inside a transaction, so this runs on
  // an autocommit connection.
  private void preparePlan(boolean fullscan) {
    try (Connection connection = fixture.openConnection();
        Statement statement = connection.createStatement()) {
      if (fullscan) statement.execute("UPDATE STATISTICS scheduler_job_queue WITH FULLSCAN");
      statement.execute("ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private void checkClaims(boolean hydrated, NodeTagFilter tags, ExecutionTargetFilter targets) {
    holder.getTransaction().begin();
    List<UUID> first = claim(holder, hydrated, 5, tags, targets);
    assertEquals(5, first.size());
    // Marking the claimed rows RUNNING turns their U locks into X locks. A U lock left over is on
    // a row the claim read but did not take.
    assertNoUpdateLocksLeft();
    if (!tags.isUnfiltered() || !targets.isAny()) {
      for (UUID id : first) {
        assertEquals(
            "wanted",
            holder
                .createNativeQuery(
                    "SELECT execution_target FROM scheduler_job_queue WHERE job_id = ?")
                .setParameter(1, UuidByteArrayConverter.toBytes(id))
                .getSingleResult());
      }
    }
    List<UUID> second = new ArrayList<>();
    assertEquals(
        List.of(),
        waited(
            store(other),
            Map.of("second claim", s -> second.addAll(claim(other, hydrated, 5, tags, targets)))));
    assertEquals(5, second.size());
    assertFalse(second.stream().anyMatch(first::contains));
  }

  private List<UUID> claim(
      EntityManager em,
      boolean hydrated,
      int limit,
      NodeTagFilter tags,
      ExecutionTargetFilter targets) {
    var store = store(em);
    return hydrated
        ? store.claimNextBatch(limit, "node", tags).stream().map(j -> j.getId()).toList()
        : store
            .claimNextBatchOptimized(JobExecutionType.SINGLE, limit, "node", tags, targets)
            .stream()
            .map(j -> j.id())
            .toList();
  }

  // Lists "index: count" for the U key locks the holder holds on the queue.
  private void assertNoUpdateLocksLeft() {
    @SuppressWarnings("unchecked")
    List<Object> indexes =
        holder
            .createNativeQuery(
                """
                SELECT i.name + ': ' + CAST(COUNT(*) AS VARCHAR(10))
                FROM sys.dm_tran_locks l
                JOIN sys.partitions p ON p.hobt_id = l.resource_associated_entity_id
                JOIN sys.indexes i ON i.object_id = p.object_id AND i.index_id = p.index_id
                WHERE l.request_session_id = @@SPID AND l.resource_type = 'KEY'
                  AND l.request_mode = 'U' AND p.object_id = OBJECT_ID('scheduler_job_queue')
                GROUP BY i.name
                ORDER BY i.name
                """)
            .getResultList();
    assertEquals(
        List.of(),
        indexes.stream().map(String::valueOf).toList(),
        "queue indexes whose keys the open claim U-locked without claiming them");
  }
}
