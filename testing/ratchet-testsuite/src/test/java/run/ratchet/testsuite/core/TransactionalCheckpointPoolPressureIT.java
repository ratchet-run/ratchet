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
package run.ratchet.testsuite.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestRuntimeConfig;
import run.ratchet.testsuite.app.TransactionalUnitProbe;
import run.ratchet.testsuite.app.TransactionalUnitProbe.Attempt;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.DataSourceStrategyFactory;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** A transactional unit must finish with one physical connection available. */
@DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
class TransactionalCheckpointPoolPressureIT extends BaseRatchetIT {
  private static final String DATASOURCE_NAME = "ratchet.test.pressure.datasource";
  private static final String POOL_SIZE = "ratchet.test.pressure.pool-size";

  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private JobAuditStore audit;
  @Inject private PayloadSerializer serializer;
  @Inject private TransactionalUnitProbe probe;
  private final Map<UUID, String> expected = new LinkedHashMap<>();

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    var datasource = DataSourceStrategyFactory.create();
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(
            TestJobService.class, TransactionalUnitProbe.class, Attempt.class, Reservations.class)
        .addStoreInfrastructure()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType,
                Map.of(
                    DATASOURCE_NAME, datasource.jtaDataSourceName(),
                    POOL_SIZE, Integer.toString(datasource.maximumPoolSize()))),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @AfterEach
  void cancelAndDrain() {
    TransactionalUnitProbe.pressureStart.countDown();
    try {
      // Stop every job before the shared probe state is cleared. A job that started later would
      // find no Attempt, and a running one would write pages after the cleanup.
      for (UUID id : expected.keySet()) jobs.cancelJob(id);
      await()
          .atMost(Duration.ofSeconds(60))
          .until(() -> expected.keySet().stream().allMatch(this::stopped));
    } finally {
      TransactionalUnitProbe.reset();
      probe.clear();
      expected.clear();
    }
  }

  /** A job has stopped once it is terminal and its latest attempt, if it began one, has ended. */
  private boolean stopped(UUID id) {
    JobStatus status = store.findById(id).orElseThrow().getStatus();
    if (!status.isTerminal()) return false;
    // A job canceled before an attempt began has no execution record, and it will not run.
    return audit
        .findLatestExecution(id)
        .map(execution -> execution.getStatus() != ExecutionStatus.RUNNING)
        .orElse(status == JobStatus.CANCELED);
  }

  @Test
  void transactionalCheckpointsSurvivePoolPressure() throws Exception {
    probe.clear();
    TransactionalUnitProbe.reset();
    TransactionalUnitProbe.pressureStart = new CountDownLatch(1);
    String key = UUID.randomUUID().toString();
    TransactionalUnitProbe.prepare(key);
    UUID id =
        jobs.enqueue(() -> TransactionalUnitProbe.execute(key, "pressure"))
            .withMaxRetries(0)
            .submit()
            .id();
    expected.put(id, key);

    TestRuntimeConfig config = new TestRuntimeConfig();
    String datasourceName = config.get(DATASOURCE_NAME, DATASOURCE_NAME).orElseThrow();
    int poolSize = Integer.parseInt(config.get(POOL_SIZE, POOL_SIZE).orElseThrow());
    DataSource datasource = InitialContext.doLookup(datasourceName);
    try (Reservations reservations = new Reservations()) {
      for (int i = 1; i < poolSize; i++) {
        reservations.connections.add(datasource.getConnection());
      }
      TransactionalUnitProbe.pressureStart.countDown();
      await()
          .atMost(Duration.ofSeconds(25))
          .untilAsserted(
              () ->
                  assertEquals(JobStatus.SUCCEEDED, store.findById(id).orElseThrow().getStatus()));
    }

    var states = store.capability(JobStateStore.class).orElseThrow();
    expected.forEach(
        (jobId, checkpointKey) -> {
          for (int page = 0; page < 3; page++) {
            assertEquals(1, probe.countPage(checkpointKey, page), checkpointKey);
          }
          var row = states.getState(jobId, "ratchet.checkpoint").orElseThrow();
          assertEquals(
              "done", serializer.deserialize(row.json(), Map.class).get("value"), jobId.toString());
        });
  }

  static final class Reservations implements AutoCloseable {
    private final List<Connection> connections = new ArrayList<>();

    @Override
    public void close() throws SQLException {
      SQLException failure = null;
      for (Connection connection : connections) {
        try {
          connection.close();
        } catch (SQLException closeFailure) {
          if (failure == null) failure = closeFailure;
          else failure.addSuppressed(closeFailure);
        }
      }
      if (failure != null) throw failure;
    }
  }
}
