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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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
import run.ratchet.testsuite.app.TransactionalUnitProbe;
import run.ratchet.testsuite.app.TransactionalUnitProbe.Attempt;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** A burst larger than both pools must complete its units without nested connections. */
@DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
class TransactionalCheckpointPoolPressureIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private JobAuditStore audit;
  @Inject private PayloadSerializer serializer;
  @Inject private TransactionalUnitProbe probe;
  private final Map<UUID, String> expected = new LinkedHashMap<>();

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(TestJobService.class, TransactionalUnitProbe.class, Attempt.class)
        .addStoreInfrastructure()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType, Map.of("ratchet.thread-pool.size.single", "64")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @AfterEach
  void releaseAndDrain() {
    TransactionalUnitProbe.pressureStart.countDown();
    try {
      await()
          .atMost(Duration.ofSeconds(40))
          .untilAsserted(
              () -> {
                for (UUID id : expected.keySet()) {
                  assertNotEquals(
                      ExecutionStatus.RUNNING,
                      audit.findLatestExecution(id).orElseThrow().getStatus());
                }
              });
    } finally {
      TransactionalUnitProbe.reset();
      probe.clear();
      expected.clear();
    }
  }

  @Test
  void transactionalCheckpointsSurvivePoolPressure() {
    probe.clear();
    TransactionalUnitProbe.reset();
    // 128 jobs exceed the default datasource pool and the 64 single-job workers.
    TransactionalUnitProbe.pressureStart = new CountDownLatch(1);
    try {
      for (int n = 0; n < 128; n++) {
        String key = UUID.randomUUID().toString();
        TransactionalUnitProbe.prepare(key);
        UUID id =
            jobs.enqueue(() -> TransactionalUnitProbe.execute(key, "pressure"))
                .withMaxRetries(0)
                .submit()
                .id();
        expected.put(id, key);
      }
    } finally {
      TransactionalUnitProbe.pressureStart.countDown();
    }
    await()
        .atMost(Duration.ofSeconds(40))
        .untilAsserted(
            () -> {
              for (UUID id : expected.keySet()) {
                assertEquals(
                    JobStatus.SUCCEEDED,
                    store.findById(id).orElseThrow().getStatus(),
                    id.toString());
              }
            });
    var states = store.capability(JobStateStore.class).orElseThrow();
    expected.forEach(
        (id, key) -> {
          for (int page = 0; page < 3; page++) assertEquals(1, probe.countPage(key, page), key);
          var row = states.getState(id, "ratchet.checkpoint").orElseThrow();
          assertEquals(
              "done", serializer.deserialize(row.json(), Map.class).get("value"), id.toString());
        });
  }
}
