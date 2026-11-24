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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.naming.InitialContext;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
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
                dbType, Map.of(POOL_SIZE, Integer.toString(datasource.maximumPoolSize()))),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @AfterEach
  void cancelAndDrain() {
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
    TestRuntimeConfig config = new TestRuntimeConfig();
    int poolSize = Integer.parseInt(config.get(POOL_SIZE, POOL_SIZE).orElseThrow());
    ThreadFactory threadFactory =
        InitialContext.doLookup(RatchetOptions.defaults().execution().threadFactoryJndi());
    String key = UUID.randomUUID().toString();
    Attempt attempt = TransactionalUnitProbe.prepare(key);
    UUID id =
        jobs.enqueue(() -> TransactionalUnitProbe.execute(key, "pressure"))
            .withMaxRetries(0)
            .submit()
            .id();
    expected.put(id, key);
    try (Reservations reservations = new Reservations()) {
      reservations.reserve(
          threadFactory, probe, sessionQuery(TestRuntimeConfig.dbType()), poolSize - 1);
      // Submission can use more than one connection. Only checkpoint work runs under pressure.
      attempt.release.countDown();
      await()
          .atMost(Duration.ofSeconds(25))
          .untilAsserted(
              () ->
                  assertEquals(JobStatus.SUCCEEDED, store.findById(id).orElseThrow().getStatus()));
    } finally {
      attempt.release.countDown();
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

  private static String sessionQuery(String dbType) {
    return switch (dbType) {
      case "mysql" -> "SELECT CONNECTION_ID()";
      case "postgresql" -> "SELECT pg_backend_pid()";
      case "oracle" -> "SELECT TO_NUMBER(SYS_CONTEXT('USERENV', 'SID')) FROM dual";
      case "sqlserver" -> "SELECT CAST(@@SPID AS INT)";
      default -> throw new IllegalArgumentException("Unsupported SQL store: " + dbType);
    };
  }

  static final class Reservations implements AutoCloseable {
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<CompletableFuture<Void>> returned = new ArrayList<>();

    void reserve(
        ThreadFactory threads, TransactionalUnitProbe probe, String sessionQuery, int count)
        throws InterruptedException, ExecutionException, TimeoutException {
      List<CompletableFuture<Long>> started = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        var ready = new CompletableFuture<Long>();
        var closed = new CompletableFuture<Void>();
        Thread thread =
            threads.newThread(
                () -> {
                  try {
                    // Use the same enlisted entity manager as a job's transactional unit. A direct
                    // datasource lookup can have different connection-sharing or authentication
                    // rules.
                    probe.reserveConnection(sessionQuery, ready, release);
                    closed.complete(null);
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    ready.completeExceptionally(interrupted);
                    closed.completeExceptionally(interrupted);
                  } catch (RuntimeException | Error failure) {
                    ready.completeExceptionally(failure);
                    closed.completeExceptionally(failure);
                  }
                });
        thread.start();
        started.add(ready);
        returned.add(closed);
      }
      CompletableFuture.allOf(started.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
      Set<Long> sessions = new HashSet<>();
      for (var ready : started) sessions.add(ready.join());
      assertEquals(
          count, sessions.size(), "Each reservation must hold a distinct database session");
    }

    @Override
    public void close() throws InterruptedException, ExecutionException, TimeoutException {
      release.countDown();
      CompletableFuture.allOf(returned.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
    }
  }
}
