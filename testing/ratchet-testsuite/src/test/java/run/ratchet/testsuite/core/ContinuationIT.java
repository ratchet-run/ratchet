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
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.ContinuationJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Continued pages consume no retry attempts and preserve checkpoint order. */
class ContinuationIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private JobQueryService queries;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(ContinuationJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void pagesContinueWithNoRetryBudget() {
    JobHandle handle = jobs.enqueue(ContinuationJob::pages).withMaxRetries(0).submit();
    try {
      JobAssertions.assertJobCompleted(store, handle);
      var job = store.findById(handle.id()).orElseThrow();
      assertEquals(JobStatus.SUCCEEDED, job.getStatus());
      assertEquals(0, job.getAttempts());
      assertEquals(List.of(0, 1, 2, 3, 4), ContinuationJob.PAGES.get(handle.id()));
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                var history = queries.getJobDetail(handle.id()).orElseThrow().executionHistory();
                assertEquals(5, history.size());
                assertTrue(history.stream().allMatch(row -> row.attempt() == 1 && row.succeeded()));
              });
    } finally {
      ContinuationJob.PAGES.remove(handle.id());
    }
  }
}
