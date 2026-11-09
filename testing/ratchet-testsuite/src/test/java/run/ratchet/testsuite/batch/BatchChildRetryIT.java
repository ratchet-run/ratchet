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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchRetryGate;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class BatchChildRetryIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private JobCrudStore jobs;
  @Inject private BatchStore batches;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(BatchRetryGate.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetGate() {
    BatchRetryGate.reset();
  }

  @Test
  void retryChildKeepsBatchPendingUntilOtherChildEnds() {
    JobHandle parent =
        scheduler
            .enqueueBatch("retry-child-counters")
            .withMaxRetries(0)
            .withTimeout(Duration.ofSeconds(60))
            .forEach(List.of("A", "B"), BatchRetryGate::process)
            .submit();
    try {
      await().atMost(Duration.ofSeconds(20)).until(BatchRetryGate::isBlocked);
      UUID child = awaitFailedChild(parent.id());
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () ->
                  assertEquals(
                      1, batches.findBatchById(parent.id()).orElseThrow().getFailedItems()));

      assertTrue(scheduler.retryJob(child));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> {
                assertEquals(JobStatus.SUCCEEDED, jobs.getJobStatus(child));
                assertEquals(
                    1, batches.findBatchById(parent.id()).orElseThrow().getCompletedItems());
              });
      await()
          .during(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(3))
          .untilAsserted(
              () -> {
                assertTrue(BatchRetryGate.isBlocked());
                assertEquals(JobStatus.PENDING, jobs.getJobStatus(parent.id()));
                assertEquals(0, batches.findBatchById(parent.id()).orElseThrow().getFailedItems());
              });
    } finally {
      BatchRetryGate.release();
      JobAssertions.assertBatchTerminated(jobs, parent, Duration.ofSeconds(20));
    }
    JobAssertions.assertBatchSucceeded(jobs, parent, Duration.ofSeconds(20));
    assertEquals(0, batches.findBatchById(parent.id()).orElseThrow().getFailedItems());
  }

  @Test
  void finishedFailedBatchRejectsChildAndParentRetry() {
    JobHandle parent =
        scheduler
            .enqueueBatch("closed-batch-retry")
            .withMaxRetries(0)
            .forEach(List.of("A"), BatchRetryGate::process)
            .submit();
    JobAssertions.assertBatchTerminated(jobs, parent, Duration.ofSeconds(20));
    assertEquals(JobStatus.FAILED, jobs.getJobStatus(parent.id()));
    UUID child = awaitFailedChild(parent.id());

    assertFalse(scheduler.retryJob(child));
    assertFalse(scheduler.retryJob(parent.id()));
    assertEquals(JobStatus.FAILED, jobs.getJobStatus(child));
    assertEquals(JobStatus.FAILED, jobs.getJobStatus(parent.id()));
    assertEquals(1, batches.findBatchById(parent.id()).orElseThrow().getFailedItems());
  }

  private UUID awaitFailedChild(UUID parentId) {
    return await()
        .atMost(Duration.ofSeconds(20))
        .until(
            () ->
                jobs.findDependants(parentId, JobCrudStore.DEFAULT_PAGE_LIMIT, 0).stream()
                    .filter(job -> job.getJobType() == JobExecutionType.BATCH_CHILD)
                    .filter(job -> job.getStatus() == JobStatus.FAILED)
                    .map(JobEntity::getId)
                    .findFirst()
                    .orElse(null),
            id -> id != null);
  }
}
