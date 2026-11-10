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
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.CheckpointJob;
import run.ratchet.testsuite.app.CheckpointTransactions;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** A burst larger than the datasource and worker pools must not need nested connections. */
class CheckpointPoolPressureIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private PayloadSerializer serializer;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(CheckpointJob.class, CheckpointTransactions.class, TestJobService.class)
        .addStoreInfrastructure()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType, Map.of("ratchet.thread-pool.size.single", "64")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @Test
  void committedCheckpointsSurvivePoolPressure() {
    // 128 exceeds the datasource default (20) and this deployment's 64 single-job workers.
    Map<UUID, String> expected = new LinkedHashMap<>();
    CheckpointJob.pressureStart = new CountDownLatch(1);
    try {
      for (int n = 0; n < 128; n++) {
        String value = "done-" + n;
        UUID id = jobs.enqueue(() -> CheckpointJob.pressure(value)).withMaxRetries(0).submit().id();
        expected.put(id, value);
      }
    } finally {
      // Start the submitted jobs as one burst, including when submission fails partway through.
      CheckpointJob.pressureStart.countDown();
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
        (id, value) -> {
          var row = states.getState(id, "ratchet.checkpoint").orElseThrow();
          assertEquals(
              value, serializer.deserialize(row.json(), Map.class).get("value"), id.toString());
        });
  }
}
