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
package run.ratchet.testsuite.batch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.SerializableBinaryOperator;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.TypeRef;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchControlGate;
import run.ratchet.testsuite.app.BatchResultJobs;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class BatchResultIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private JobCrudStore jobs;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(BatchResultJobs.class, BatchControlGate.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void reset() {
    BatchResultJobs.reset();
    BatchControlGate.reset();
  }

  @Test
  void twentyFiveChildrenDeliverTheirSum() {
    JobHandle handle =
        scheduler
            .enqueueBatch("sum")
            .reducing(0L, Long::sum)
            .forEach(IntStream.rangeClosed(1, 25).boxed().toList(), BatchResultJobs::partial)
            .thenOnBatchSuccess(BatchResultJobs::receive)
            .submit();
    assertDelivered(handle, 325L);
    assertEquals("325", jobs.findById(handle.id()).orElseThrow().getJobResult());
  }

  @Test
  void parameterizedWordCountIsDelivered() {
    JobHandle handle =
        scheduler
            .enqueueBatch("word-count")
            .reducing(Map.<String, Long>of(), BatchResultJobs::merge)
            .forEach(List.of("one", "two", "one"), BatchResultJobs::word)
            .thenOnBatchSuccess(BatchResultJobs::receiveWords)
            .submit();
    assertDelivered(handle, Map.of("one", 2L, "two", 1L));
  }

  @Test
  void explicitTypeAllowsGenericUnion() {
    JobHandle handle =
        scheduler
            .enqueueBatch("union")
            .reducing(new TypeRef<Set<String>>() {}, Set.of(), BatchResultJobs::union)
            .forEach(List.of("one", "two", "one"), BatchResultJobs::singleton)
            .thenOnBatchSuccess(BatchResultJobs::receiveSet)
            .submit();
    assertDelivered(handle, Set.of("one", "two"));
  }

  @Test
  void emptyBatchDeliversIdentityWithoutCallingCombiner() {
    JobHandle handle =
        scheduler
            .enqueueBatch("empty")
            .reducing(17L, BatchResultJobs::sum)
            .forEach(List.<Integer>of(), BatchResultJobs::partial)
            .thenOnBatchSuccess(BatchResultJobs::receive)
            .submit();
    assertDelivered(handle, 17L);
    assertEquals(0, BatchResultJobs.folds());
  }

  @Test
  void nullPartialFailsBatchAndRunsFailureCallback() {
    assertFailed(BatchResultJobs::nothing, BatchResultJobs::sum);
  }

  @Test
  void throwingCombinerFailsBatchAndRunsFailureCallback() {
    assertFailed(BatchResultJobs::partial, BatchResultJobs::broken);
  }

  @Test
  void failedChildNeverCallsCombiner() {
    assertFailed(BatchResultJobs::fail, BatchResultJobs::sum);
    assertEquals(0, BatchResultJobs.folds());
  }

  @Test
  void pausedBatchResumesAndDeliversResult() {
    JobHandle handle =
        scheduler
            .enqueueBatch("paused-result")
            .reducing(0L, Long::sum)
            .forEach(IntStream.rangeClosed(1, 80).boxed().toList(), BatchResultJobs::gated)
            .thenOnBatchSuccess(BatchResultJobs::receive)
            .submit();
    try {
      await().atMost(Duration.ofSeconds(20)).until(() -> BatchControlGate.started() > 0);
      assertTrue(scheduler.pauseJob(handle.id()));
      BatchControlGate.release();
      assertEquals(0, BatchResultJobs.successes());
      assertTrue(scheduler.resumeJob(handle.id()));
      assertDelivered(handle, 3240L);
    } finally {
      BatchControlGate.release();
      scheduler.resumeJob(handle.id());
      JobAssertions.assertBatchTerminated(jobs, handle, Duration.ofSeconds(60));
    }
  }

  @Test
  void twoThousandChildrenSpanMultipleResultPages() {
    JobHandle handle =
        scheduler
            .enqueueBatch("paged-sum")
            .reducing(0L, Long::sum)
            .forEach(IntStream.rangeClosed(1, 2000).boxed().toList(), BatchResultJobs::partial)
            .thenOnBatchSuccess(BatchResultJobs::receive)
            .submit();
    assertDelivered(handle, 2001000L);
  }

  private void assertDelivered(JobHandle handle, Object expected) {
    JobAssertions.assertBatchSucceeded(jobs, handle, Duration.ofMinutes(2));
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertEquals(1, BatchResultJobs.successes());
              assertEquals(expected, BatchResultJobs.received());
              assertEquals(0, BatchResultJobs.failures());
            });
  }

  private void assertFailed(
      SerializableFunction<Integer, Long> child, SerializableBinaryOperator<Long> combiner) {
    JobHandle handle =
        scheduler
            .enqueueBatch("failed-result")
            .reducing(0L, combiner)
            .forEach(List.of(1, 2, 3), child)
            .thenOnBatchSuccess(BatchResultJobs::receive)
            .thenOnBatchFailure(BatchResultJobs::failure)
            .submit();
    JobAssertions.assertBatchTerminated(jobs, handle, Duration.ofSeconds(60));
    assertEquals(JobStatus.FAILED, jobs.getJobStatus(handle.id()));
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertEquals(1, BatchResultJobs.failures());
              assertEquals(0, BatchResultJobs.successes());
            });
  }
}
