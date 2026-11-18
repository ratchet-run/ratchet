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
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

/**
 * The store requires RCSI; this test only checks that a deployment running without it in WARN mode
 * skips a row another transaction holds during the boosted candidate read rather than waiting.
 */
class SqlserverBoostedClaimRcsiOffTest {
  private static final RatchetOptions OPTIONS =
      RatchetOptions.builder()
          .store(
              s ->
                  s.priorityBoostIntervalMinutes(15)
                      .isolationCheckMode(RatchetOptions.IsolationCheckMode.WARN))
          .build();

  @Test
  void boostedCandidateReadSkipsExclusiveLockWithRcsiOff() throws Exception {
    var container = MssqlContainers.shared();
    String database = "ratchet_rcsi_off_" + UUID.randomUUID().toString().replace("-", "");
    String baseUrl = container.getJdbcUrl().replaceAll("(?i);databaseName=[^;]*", "");
    String url = baseUrl + ";databaseName=" + database;
    Connection holder = null;
    EntityManagerFactory emf = null;
    EntityManager seed = null;
    EntityManager other = null;
    try (Connection master =
            DriverManager.getConnection(
                baseUrl + ";databaseName=master",
                container.getUsername(),
                container.getPassword());
        var statement = master.createStatement()) {
      statement.execute("CREATE DATABASE [" + database + "]");
      try {
        statement.execute("ALTER DATABASE [" + database + "] SET READ_COMMITTED_SNAPSHOT OFF");
        MssqlContainers.applySchema(url, container.getUsername(), container.getPassword());
        var properties = new HashMap<String, Object>();
        properties.put("jakarta.persistence.jdbc.url", url + ";lockTimeout=1000");
        properties.put("jakarta.persistence.jdbc.user", container.getUsername());
        properties.put("jakarta.persistence.jdbc.password", container.getPassword());
        properties.put("hibernate.hbm2ddl.auto", "none");
        properties.put("hibernate.type.preferred_uuid_jdbc_type", "BINARY");
        properties.put("hibernate.connection.pool_size", "4");
        properties.put("hibernate.connection.isolation", "2");
        emf = Persistence.createEntityManagerFactory("ratchet-sqlserver-tck", properties);
        seed = emf.createEntityManager();
        other = emf.createEntityManager();
        assertEquals(database, seed.createNativeQuery("SELECT DB_NAME()").getSingleResult());
        assertEquals(
            0,
            ((Number)
                    seed.createNativeQuery(
                            """
                            SELECT CAST(is_read_committed_snapshot_on AS int)
                            FROM sys.databases WHERE name = DB_NAME()
                            """)
                        .getSingleResult())
                .intValue());

        List<UUID> ids = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        seed.getTransaction().begin();
        var seedStore = store(seed);
        var fixture = new SqlserverTestFixture();
        Instant earliest = Instant.now().minusSeconds(600);
        for (int i = 0; i < ids.size(); i++) {
          var job = fixture.newPendingJob();
          job.setId(ids.get(i));
          job.setJobType(JobExecutionType.SINGLE);
          job.setPriority(JobPriority.LOWEST);
          job.setScheduledTime(earliest.plusSeconds(i * 60L));
          seedStore.create(job);
        }
        seed.getTransaction().commit();

        holder = DriverManager.getConnection(url, container.getUsername(), container.getPassword());
        holder.setAutoCommit(false);
        // Removing the earliest row from the filtered PENDING index holds an X lock on its key.
        try (var update =
            holder.prepareStatement(
                "UPDATE scheduler_job_queue SET status = 'RUNNING' WHERE job_id = ?")) {
          update.setBytes(1, UuidByteArrayConverter.toBytes(ids.get(0)));
          assertEquals(1, update.executeUpdate());
        }

        var claimStore = store(other);
        other.getTransaction().begin();
        var claims =
            claimStore.claimNextBatchOptimized(
                JobExecutionType.SINGLE,
                10,
                "node",
                NodeTagFilter.NONE,
                ExecutionTargetFilter.any());
        assertEquals(2, claims.size());
        assertEquals(
            Set.of(ids.get(1), ids.get(2)),
            claims.stream().map(j -> j.id()).collect(Collectors.toSet()));
        other.getTransaction().rollback();
      } finally {
        try {
          if (holder != null) {
            try {
              holder.rollback();
            } finally {
              holder.close();
            }
          }
        } finally {
          try {
            closeSession(other);
          } finally {
            try {
              closeSession(seed);
            } finally {
              try {
                if (emf != null) emf.close();
              } finally {
                try {
                  statement.execute(
                      "ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
                } finally {
                  statement.execute("DROP DATABASE [" + database + "]");
                }
              }
            }
          }
        }
      }
    }
  }

  private static SqlserverJobStoreImpl store(EntityManager em) {
    var store = new SqlserverJobStoreImpl(() -> em, mock(MetricsCollector.class), OPTIONS);
    store.checkIsolationLevel();
    return store;
  }

  private static void closeSession(EntityManager em) {
    if (em == null) return;
    try {
      if (em.getTransaction().isActive()) em.getTransaction().rollback();
    } finally {
      em.close();
    }
  }
}
