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
package run.ratchet.micronaut.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.jdbc.DataSourceResolver;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.hibernate.HibernateTransactionManager;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.hibernate.SessionFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.ri.cdi.internal.JsonbPayloadSerializer;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.schema.RatchetJpaModel;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;

/**
 * Boots Micronaut's real Hibernate infrastructure against migrated PostgreSQL, without a poller.
 */
@Testcontainers
class RatchetJpaBootTest {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

  @ParameterizedTest
  @ValueSource(strings = {"default", "jobs"})
  void mapsMigratesAndHonorsStoreTransactions(String datasource) throws Exception {
    var options =
        RatchetOptionsFactory.builderFromEnvironment()
            .schema(schema -> schema.autoMigrate(true))
            .build();
    // Supply only core types, independent of integration bean names. The mock runtime prevents
    // a scheduler/poller from consuming fixture jobs when the core startup listener is present.
    try (RuntimeContextInstallation installation = RuntimeContextInstallation.claim()) {
      installation.setSerializer(new JsonbPayloadSerializer());
      try (ApplicationContext context =
          ApplicationContext.builder()
              .environments("test")
              .singletons(
                  options, new NoOpMetricsCollector(), installation, mock(RatchetRuntime.class))
              .properties(properties(datasource))
              .start()) {
        // The raw pool: counts must see only committed rows, from a separate connection.
        DataSource dataSource =
            context
                .getBean(DataSourceResolver.class)
                .resolve(context.getBean(DataSource.class, Qualifiers.byName(datasource)));
        SessionFactory factory =
            context.getBean(SessionFactory.class, Qualifiers.byName(datasource));
        HibernateTransactionManager transactions =
            context.getBean(HibernateTransactionManager.class, Qualifiers.byName(datasource));
        JobStore store = context.getBean(JobStore.class);
        assertTrue(Proxy.isProxyClass(store.getClass()));
        assertSame(store, store.capability(LockStore.class).orElseThrow());
        assertEquals(
            RatchetJpaModel.ENTITY_CLASS_NAMES.size(), factory.getMetamodel().getEntities().size());
        assertTrue(count(dataSource, "select count(*) from ratchet_schema_version") > 0);
        assertEquals(JobEntity.class, factory.getMetamodel().entity(JobEntity.class).getJavaType());

        JobEntity job = job();
        String nodeId = "tx-" + UUID.randomUUID();
        String lockName = "tx-" + UUID.randomUUID();
        LockStore locks = store.capability(LockStore.class).orElseThrow();
        assertThrows(
            Abort.class,
            () ->
                transactions.execute(
                    TransactionDefinition.DEFAULT,
                    status -> {
                      store.create(job); // Class default REQUIRED: joins the outer transaction.
                      store.upsertHeartbeat(nodeId, Instant.now()); // Implementation REQUIRES_NEW.
                      assertTrue(
                          locks.tryLock(
                              lockName, Duration.ofMinutes(1), nodeId)); // REQUIRES_NEW capability.
                      // A separate JDBC connection sees only the independently committed writes.
                      assertEquals(
                          0,
                          count(
                              dataSource,
                              "select count(*) from scheduler_job where job_id = '"
                                  + job.getId()
                                  + "'"));
                      assertEquals(
                          1,
                          count(
                              dataSource,
                              "select count(*) from scheduler_node where node_id = '"
                                  + nodeId
                                  + "'"));
                      assertEquals(
                          1,
                          count(
                              dataSource,
                              "select count(*) from scheduler_lock where lock_name = '"
                                  + lockName
                                  + "'"));
                      throw new Abort();
                    }));
        assertFalse(store.findById(job.getId()).isPresent());
        assertTrue(store.findNodeById(nodeId).isPresent());
        assertEquals(
            1,
            count(
                dataSource,
                "select count(*) from scheduler_lock where lock_name = '" + lockName + "'"));
        locks.unlock(lockName, nodeId);
        store.deleteInactiveNodesByIds(List.of(nodeId));

        JobEntity committed = job();
        try {
          store.create(committed); // REQUIRED also starts/commits its own transaction when absent.
          assertTrue(store.findById(committed.getId()).isPresent());
        } finally {
          store.delete(committed.getId());
        }
      }
    }
  }

  private static Map<String, Object> properties(String name) {
    // Micronaut must preserve the selected name across DataSource, MetadataSources,
    // SessionFactory and HibernateTransactionManager.
    Map<String, Object> properties =
        new HashMap<>(
            Map.of(
                "ratchet.jpa.datasource",
                name,
                "datasources." + name + ".url",
                POSTGRES.getJdbcUrl(),
                "datasources." + name + ".username",
                POSTGRES.getUsername(),
                "datasources." + name + ".password",
                POSTGRES.getPassword(),
                "datasources." + name + ".driver-class-name",
                "org.postgresql.Driver",
                "jpa." + name + ".properties.hibernate.hbm2ddl.auto",
                "validate"));
    if (!name.equals("default")) {
      // Fail if Ratchet accidentally selects the unselected default persistence unit.
      properties.put("datasources.default.url", POSTGRES.getJdbcUrl());
      properties.put("datasources.default.username", POSTGRES.getUsername());
      properties.put("datasources.default.password", POSTGRES.getPassword());
      properties.put("datasources.default.driver-class-name", "org.postgresql.Driver");
      properties.put("jpa.default.enabled", false);
    }
    return properties;
  }

  private static JobEntity job() {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now().plus(Duration.ofDays(1)));
    job.setJobType(JobExecutionType.SINGLE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("example.TestJob", "execute", "()V", false, List.of(), null));
    return job;
  }

  private static long count(DataSource dataSource, String sql) throws SQLException {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static final class Abort extends RuntimeException {}
}
