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
import java.util.UUID;
import java.util.stream.IntStream;
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
import run.ratchet.testsuite.app.BatchControlGate;
import run.ratchet.testsuite.app.BatchRetryGate;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class BatchControlIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private JobCrudStore jobs;
  @Inject private BatchStore batches;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(BatchControlGate.class, BatchRetryGate.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetGates() {
    BatchControlGate.reset();
    BatchRetryGate.reset();
  }

  private JobHandle submit() {
    return scheduler
        .enqueueBatch("batch-controls")
        .withMaxRetries(0)
        .withTimeout(Duration.ofSeconds(90))
        .forEach(IntStream.range(0, 40).boxed().toList(), BatchControlGate::process)
        .thenOnBatchSuccess(BatchControlGate::success)
        .thenOnBatchFailure(BatchControlGate::failure)
        .submit();
  }

  private List<JobEntity> dependants(UUID id) {
    return jobs.findDependants(id, JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
  }

  private void awaitBlocked(JobHandle parent) {
    await()
        .atMost(Duration.ofSeconds(20))
        .until(
            () ->
                BatchControlGate.started() > 0
                    && dependants(parent.id()).stream()
                        .anyMatch(
                            child ->
                                child.getJobType() == JobExecutionType.BATCH_CHILD
                                    && child.getStatus() == JobStatus.PENDING));
  }

  @Test
  void cancelParentLeavesRunningChildrenAndCancelsCallbacks() {
    JobHandle parent = submit();
    try {
      awaitBlocked(parent);
      assertTrue(scheduler.cancelJob(parent.id()));
      assertEquals(JobStatus.CANCELED, jobs.getJobStatus(parent.id()));
      List<JobEntity> children = dependants(parent.id());
      assertEquals(
          2,
          children.stream()
              .filter(child -> child.getJobType() == JobExecutionType.WORKFLOW_BRANCH)
              .count());
      for (JobEntity child : children) {
        if (child.getJobType() == JobExecutionType.WORKFLOW_BRANCH) {
          assertEquals(JobStatus.CANCELED, child.getStatus());
        }
        assertTrue(
            child.getStatus() == JobStatus.RUNNING || child.getStatus() == JobStatus.CANCELED);
      }
      assertTrue(batches.findBatchById(parent.id()).orElseThrow().getCompletionProcessed());
    } finally {
      BatchControlGate.release();
      await()
          .atMost(Duration.ofSeconds(20))
          .until(
              () ->
                  dependants(parent.id()).stream()
                      .noneMatch(child -> child.getStatus() == JobStatus.RUNNING));
    }
    for (JobEntity child : dependants(parent.id())) {
      assertTrue(
          child.getStatus() == JobStatus.SUCCEEDED || child.getStatus() == JobStatus.CANCELED);
    }
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              assertEquals(0, BatchControlGate.successes());
              assertEquals(0, BatchControlGate.failures());
            });
  }

  @Test
  void cancelledBatchDoesNotStartAnotherAttemptAfterRunningChildrenFail() {
    JobHandle parent =
        scheduler
            .enqueueBatch("cancel-failing-children")
            .withMaxRetries(2)
            .withTimeout(Duration.ofSeconds(90))
            .forEach(IntStream.range(0, 40).boxed().toList(), BatchControlGate::processAndFail)
            .thenOnBatchSuccess(BatchControlGate::success)
            .thenOnBatchFailure(BatchControlGate::failure)
            .submit();
    List<UUID> running;
    try {
      awaitBlocked(parent);
      // Let claimed workers enter the gate before taking the cancellation snapshot.
      await()
          .during(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(10))
          .until(
              () -> {
                long count =
                    dependants(parent.id()).stream()
                        .filter(child -> child.getJobType() == JobExecutionType.BATCH_CHILD)
                        .filter(child -> child.getStatus() == JobStatus.RUNNING)
                        .count();
                return count > 0 && count < 40 && count == BatchControlGate.started();
              });
      running =
          dependants(parent.id()).stream()
              .filter(child -> child.getJobType() == JobExecutionType.BATCH_CHILD)
              .filter(child -> child.getStatus() == JobStatus.RUNNING)
              .map(JobEntity::getId)
              .toList();
      assertTrue(!running.isEmpty() && running.size() < 40);
      assertEquals(running.size(), BatchControlGate.started());
      assertTrue(scheduler.cancelJob(parent.id()));
    } finally {
      BatchControlGate.release();
      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () ->
                  dependants(parent.id()).stream()
                      .filter(child -> child.getJobType() == JobExecutionType.BATCH_CHILD)
                      .noneMatch(
                          child ->
                              child.getStatus() == JobStatus.PENDING
                                  || child.getStatus() == JobStatus.RUNNING));
    }
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              List<JobEntity> children =
                  dependants(parent.id()).stream()
                      .filter(child -> child.getJobType() == JobExecutionType.BATCH_CHILD)
                      .toList();
              assertEquals(40, children.size());
              for (JobEntity child : children) {
                assertTrue(
                    child.getStatus() == JobStatus.CANCELED
                        || child.getStatus() == JobStatus.FAILED);
              }
              for (UUID id : running) {
                assertEquals(JobStatus.FAILED, jobs.getJobStatus(id));
              }
              var batch = batches.findBatchById(parent.id()).orElseThrow();
              assertTrue(batch.getCompletionProcessed());
              assertEquals(
                  batch.getTotalItems(), batch.getFailedItems() + batch.getCompletedItems());
            });
    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(6))
        .untilAsserted(
            () -> {
              assertEquals(running.size(), BatchControlGate.started());
              assertEquals(0, BatchControlGate.successes());
              assertEquals(0, BatchControlGate.failures());
            });
  }

  @Test
  void pauseParentFencesPendingChildrenAndResumeCompletesBatch() {
    JobHandle parent = submit();
    try {
      awaitBlocked(parent);
      assertTrue(scheduler.pauseJob(parent.id()));
      List<JobEntity> children =
          dependants(parent.id()).stream()
              .filter(child -> child.getJobType() == JobExecutionType.BATCH_CHILD)
              .toList();
      assertTrue(children.stream().noneMatch(child -> child.getStatus() == JobStatus.PENDING));
      assertTrue(children.stream().anyMatch(child -> child.getStatus() == JobStatus.PAUSED));
      BatchControlGate.release();
      await()
          .atMost(Duration.ofSeconds(20))
          .until(
              () ->
                  dependants(parent.id()).stream()
                      .noneMatch(child -> child.getStatus() == JobStatus.RUNNING));
      int started = BatchControlGate.started();
      await()
          .during(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(6))
          .untilAsserted(
              () -> {
                assertEquals(started, BatchControlGate.started());
                assertEquals(JobStatus.PAUSED, jobs.getJobStatus(parent.id()));
                assertEquals(0, BatchControlGate.successes());
              });
      assertTrue(scheduler.resumeJob(parent.id()));
      JobAssertions.assertBatchSucceeded(jobs, parent, Duration.ofSeconds(30));
      await().atMost(Duration.ofSeconds(10)).until(() -> BatchControlGate.successes() == 1);
      assertEquals(40, BatchControlGate.started());
      assertEquals(0, BatchControlGate.failures());
    } finally {
      BatchControlGate.release();
      scheduler.resumeJob(parent.id());
      JobAssertions.assertBatchTerminated(jobs, parent, Duration.ofSeconds(30));
    }
  }

  @Test
  void retryParentResetsFailedChildrenOfOpenBatch() {
    JobHandle parent =
        scheduler
            .enqueueBatch("retry-parent-controls")
            .withMaxRetries(0)
            .withTimeout(Duration.ofSeconds(60))
            .forEach(List.of("A", "B"), BatchRetryGate::process)
            .thenOnBatchSuccess(BatchControlGate::success)
            .thenOnBatchFailure(BatchControlGate::failure)
            .submit();
    try {
      await().atMost(Duration.ofSeconds(20)).until(BatchRetryGate::isBlocked);
      await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> batches.findBatchById(parent.id()).orElseThrow().getFailedItems() == 1);
      assertTrue(scheduler.retryJob(parent.id()));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> {
                assertEquals(0, batches.findBatchById(parent.id()).orElseThrow().getFailedItems());
                assertEquals(
                    1, batches.findBatchById(parent.id()).orElseThrow().getCompletedItems());
                assertEquals(JobStatus.PENDING, jobs.getJobStatus(parent.id()));
              });
    } finally {
      BatchRetryGate.release();
      JobAssertions.assertBatchTerminated(jobs, parent, Duration.ofSeconds(20));
    }
    JobAssertions.assertBatchSucceeded(jobs, parent, Duration.ofSeconds(20));
    assertEquals(2, batches.findBatchById(parent.id()).orElseThrow().getCompletedItems());
    assertEquals(0, batches.findBatchById(parent.id()).orElseThrow().getFailedItems());
    await().atMost(Duration.ofSeconds(10)).until(() -> BatchControlGate.successes() == 1);
    assertEquals(0, BatchControlGate.failures());
  }
}
