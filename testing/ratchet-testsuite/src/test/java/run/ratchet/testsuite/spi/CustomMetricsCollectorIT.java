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
package run.ratchet.testsuite.spi;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CountingMetricsCollector;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestMetricsCollectorAdapter;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates that a custom {@link MetricsCollector} alternative receives job metrics. */
class CustomMetricsCollectorIT extends BaseRatchetIT {

  @Inject private MetricsCollector metricsCollector;

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            CountingMetricsCollector.class,
            FailingJob.class,
            SimpleJob.class,
            TestJobService.class,
            TestMetricsCollectorAdapter.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    CountingMetricsCollector.resetCounts();
    FailingJob.resetCount();
    SimpleJob.resetCount();
  }

  @Test
  void customMetricsCollector_shouldReceiveJobMetrics() {
    assertInstanceOf(CountingMetricsCollector.class, metricsCollector);

    var handle = jobService.enqueueNow(SimpleJob::execute);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, SimpleJob.getInvocationCount(), "SimpleJob must execute exactly once");

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              List<CountingMetricsCollector.StartedMetric> started =
                  CountingMetricsCollector.startedEvents().stream()
                      .filter(event -> event.jobId().equals(handle.id()))
                      .toList();
              List<CountingMetricsCollector.CompletedMetric> completed =
                  CountingMetricsCollector.completedEvents().stream()
                      .filter(event -> event.jobId().equals(handle.id()))
                      .toList();

              assertEquals(1, started.size(), "Expected exactly one jobStarted event");
              assertEquals(JobType.SINGLE, started.get(0).type());
              assertEquals(JobPriority.NORMAL, started.get(0).priority());

              assertEquals(1, completed.size(), "Expected exactly one jobCompleted event");
              assertEquals(JobType.SINGLE, completed.get(0).type());
              assertTrue(
                  completed.get(0).executionTimeMs() >= 0,
                  "Execution time metric must be non-negative");
              assertTrue(
                  CountingMetricsCollector.failedEvents().stream()
                      .noneMatch(event -> event.jobId().equals(handle.id())),
                  "Successful job must not emit jobFailed");
            });
  }

  @Test
  void customMetricsCollector_shouldReceiveFailedJobMetrics() {
    assertInstanceOf(CountingMetricsCollector.class, metricsCollector);

    var handle = jobService.enqueue(FailingJob::execute).withMaxRetries(0).submit();
    JobAssertions.assertJobFailed(jobCrudStore, handle);
    assertEquals(1, FailingJob.getAttemptCount(), "FailingJob must execute exactly once");

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              List<CountingMetricsCollector.StartedMetric> started =
                  CountingMetricsCollector.startedEvents().stream()
                      .filter(event -> event.jobId().equals(handle.id()))
                      .toList();
              List<CountingMetricsCollector.FailedMetric> failed =
                  CountingMetricsCollector.failedEvents().stream()
                      .filter(event -> event.jobId().equals(handle.id()))
                      .toList();

              assertEquals(1, started.size(), "Expected exactly one jobStarted event");
              assertEquals(JobType.SINGLE, started.get(0).type());
              assertEquals(JobPriority.NORMAL, started.get(0).priority());

              assertEquals(1, failed.size(), "Expected exactly one jobFailed event");
              assertEquals(JobType.SINGLE, failed.get(0).type());
              assertEquals(1, failed.get(0).attempt());
              assertEquals(RuntimeException.class.getName(), failed.get(0).causeType());
              assertTrue(
                  CountingMetricsCollector.completedEvents().stream()
                      .noneMatch(event -> event.jobId().equals(handle.id())),
                  "Failed job must not emit jobCompleted");
            });
  }
}
