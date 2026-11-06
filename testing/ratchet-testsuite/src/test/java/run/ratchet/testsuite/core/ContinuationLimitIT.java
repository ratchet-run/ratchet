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
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.ContinuationJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * The global continuation limit ends an always-continuing job and manual retry gets a fresh budget.
 */
class ContinuationLimitIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private JobSchedulerService scheduler;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(ContinuationJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType, Map.of("ratchet.execution.max-continuations", "2")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @Test
  void limitFailsAndManualRetryGetsFreshBudget() {
    JobHandle handle = jobs.enqueue(ContinuationJob::always).withMaxRetries(0).submit();
    try {
      JobAssertions.assertJobFailed(store, handle);
      var failed = store.findById(handle.id()).orElseThrow();
      assertEquals(0, failed.getAttempts());
      assertTrue(failed.getLastError().contains("limit 2"));
      assertTrue(failed.getLastError().contains("ratchet.execution.max-continuations"));
      assertEquals(3, ContinuationJob.CALLS.get(handle.id()).get());
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                var counter =
                    store
                        .capability(JobStateStore.class)
                        .orElseThrow()
                        .getState(handle.id(), "ratchet.continuation")
                        .orElseThrow();
                assertEquals("{\"count\":0}", counter.json());
              });
      assertTrue(scheduler.retryJob(handle.id()));
      JobAssertions.assertJobFailed(store, handle);
      assertEquals(6, ContinuationJob.CALLS.get(handle.id()).get());
    } finally {
      ContinuationJob.CALLS.remove(handle.id());
    }
  }
}
