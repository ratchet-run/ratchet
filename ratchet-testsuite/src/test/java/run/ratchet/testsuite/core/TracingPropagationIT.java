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
import java.util.Map;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.RecordingTracingCollector;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies end-to-end two-phase trace context propagation: the W3C carrier map captured at enqueue
 * time reaches {@link run.ratchet.spi.TracingCollector#jobExecutionStarted} at execution time,
 * regardless of which thread picks up the job.
 *
 * <p>Uses {@link RecordingTracingCollector} to intercept both phases without a real tracing
 * backend.
 */
class TracingPropagationIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            RecordingTracingCollector.class,
            SimpleJob.class,
            FailingJob.class,
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void reset() {
    RecordingTracingCollector.reset();
    SimpleJob.resetCount();
    FailingJob.resetCount();
  }

  @Test
  void traceContext_propagatedToExecutionScope_forSingleJob() {
    Map<String, String> fakeCarrier =
        Map.of("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    RecordingTracingCollector.setContextToCapture(fakeCarrier);

    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> !RecordingTracingCollector.getReceivedParentContexts().isEmpty());

    assertEquals(
        fakeCarrier,
        RecordingTracingCollector.getReceivedParentContexts().get(0),
        "parentContext passed to jobExecutionStarted must match the carrier captured at enqueue time");
  }

  @Test
  void traceContext_propagatedToAllChainSteps() {
    Map<String, String> fakeCarrier =
        Map.of("traceparent", "00-1234567890abcdef1234567890abcdef-fedcba0987654321-01");
    RecordingTracingCollector.setContextToCapture(fakeCarrier);

    JobHandle handle =
        jobService
            .enqueue(SimpleJob::execute)
            .then(SimpleJob::execute)
            .then(SimpleJob::execute)
            .submit();

    JobAssertions.assertChainCompleted(jobCrudStore, handle, 3, Duration.ofSeconds(30));

    // All three steps must have received the enqueue-time carrier — each step was created on the
    // submitting thread while the caller's context was active.
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> RecordingTracingCollector.getReceivedParentContexts().size() >= 3);

    List<Map<String, String>> received = RecordingTracingCollector.getReceivedParentContexts();
    assertEquals(3, received.size(), "Expected one jobExecutionStarted call per chain step");
    for (int i = 0; i < received.size(); i++) {
      assertEquals(
          fakeCarrier,
          received.get(i),
          "Chain step " + (i + 1) + " parentContext must match the enqueue-time carrier");
    }
  }

  @Test
  void noActiveTrace_passesEmptyContextAndDoesNotCrash() {
    // contextToCapture is Map.of() after reset — captureCurrentContext() returns empty,
    // so nothing is stored on the entity, and jobExecutionStarted receives Map.of().
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> !RecordingTracingCollector.getReceivedParentContexts().isEmpty());

    assertTrue(
        RecordingTracingCollector.getReceivedParentContexts().get(0).isEmpty(),
        "When no trace is active, jobExecutionStarted must receive an empty parentContext");
  }

  @Test
  void executionScope_recordsSuccessAndClosesOnce() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertEquals(
                    List.of("started", "success", "close"),
                    RecordingTracingCollector.getExecutionScopeEvents()));
  }

  @Test
  void executionScope_recordsFailureAndClosesOnce() {
    JobHandle handle = jobService.enqueue(FailingJob::execute).withMaxRetries(0).submit();
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertEquals(
                    List.of("started", "failure", "close"),
                    RecordingTracingCollector.getExecutionScopeEvents()));
  }

  @Test
  void executionScope_closeIsIdempotent() {
    RecordingTracingCollector collector = new RecordingTracingCollector();
    TracingCollector.ExecutionScope scope =
        collector.jobExecutionStarted(
            UUID.randomUUID(), JobType.SINGLE, JobPriority.NORMAL, Map.of());

    scope.close();
    scope.close();

    assertEquals(
        List.of("started", "close"),
        RecordingTracingCollector.getExecutionScopeEvents(),
        "Repeated close calls should record a single close event");
  }
}
