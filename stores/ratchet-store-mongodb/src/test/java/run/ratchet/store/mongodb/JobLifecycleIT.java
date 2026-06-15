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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;

class JobLifecycleIT extends BaseDocumentStoreIT {

  @Test
  void fullLifecycle_createClaimSucceed() {
    JobEntity job = store().save(newPendingJob());
    assertNotNull(job.getId());
    assertEquals(JobStatus.PENDING, job.getStatus());

    List<JobEntity> claimed = store().claimNextBatch(1, "node-1");
    assertEquals(1, claimed.size());
    assertEquals(job.getId(), claimed.get(0).getId());
    assertEquals(JobStatus.RUNNING, claimed.get(0).getStatus());
    assertEquals("node-1", claimed.get(0).getPickedBy());

    boolean swapped =
        store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
    assertTrue(swapped);

    Optional<JobEntity> completed = store().findById(job.getId());
    assertTrue(completed.isPresent());
    assertEquals(JobStatus.SUCCEEDED, completed.get().getStatus());
  }

  @Test
  void fullLifecycle_createClaimFail() {
    JobEntity job = store().save(newPendingJob());

    List<JobEntity> claimed = store().claimNextBatch(1, "node-1");
    assertEquals(1, claimed.size());

    boolean swapped =
        store()
            .compareAndSwapStatus(
                job.getId(), JobStatus.RUNNING, JobStatus.FAILED, "Something went wrong");
    assertTrue(swapped);

    Optional<JobEntity> failed = store().findById(job.getId());
    assertTrue(failed.isPresent());
    assertEquals(JobStatus.FAILED, failed.get().getStatus());
    assertEquals("Something went wrong", failed.get().getLastError());
  }

  @Test
  void retryFlow_failIncrementRetryReclaimSucceed() {
    JobEntity job = newPendingJob();
    job.setMaxRetries(3);
    job = store().save(job);

    store().claimNextBatch(1, "node-1");
    int attempts = store().incrementRetryAttempt(job.getId());
    // Reschedule via scheduleJobRetry (RUNNING -> PENDING) to retry while preserving the attempt
    // count. compareAndSwapStatus rejects a terminal expected on every store, and
    // resetFailedToPending would zero attempts, so neither fits this flow.
    store().scheduleJobRetry(job.getId(), "first attempt", Instant.now().minusSeconds(1), attempts);

    List<JobEntity> reclaimed = store().claimNextBatch(1, "node-1");
    assertEquals(1, reclaimed.size());

    store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);

    Optional<JobEntity> result = store().findById(job.getId());
    assertTrue(result.isPresent());
    assertEquals(JobStatus.SUCCEEDED, result.get().getStatus());
    assertEquals(1, result.get().getAttempts());
  }

  @Test
  void casRejectsStaleTransition() {
    JobEntity job = store().save(newPendingJob());
    store().claimNextBatch(1, "node-1");

    // Try to transition from PENDING (stale) — should fail because it's now RUNNING
    boolean swapped =
        store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.SUCCEEDED, null);
    assertFalse(swapped);

    Optional<JobEntity> current = store().findById(job.getId());
    assertTrue(current.isPresent());
    assertEquals(JobStatus.RUNNING, current.get().getStatus());
  }

  @Test
  void concurrentCas_allowsOnlyOneTerminalTransition() throws Exception {
    JobEntity job = store().save(newPendingJob());
    store().claimNextBatch(1, "node-1");

    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> succeeded =
          executor.submit(
              () -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return store()
                    .compareAndSwapStatus(
                        job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
              });
      Future<Boolean> failed =
          executor.submit(
              () -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return store()
                    .compareAndSwapStatus(
                        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, "race loser");
              });

      start.countDown();

      int successfulTransitions =
          (succeeded.get(5, TimeUnit.SECONDS) ? 1 : 0) + (failed.get(5, TimeUnit.SECONDS) ? 1 : 0);
      assertEquals(1, successfulTransitions);

      Optional<JobEntity> current = store().findById(job.getId());
      assertTrue(current.isPresent());
      assertTrue(
          current.get().getStatus() == JobStatus.SUCCEEDED
              || current.get().getStatus() == JobStatus.FAILED);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void terminalFailure_persistsMaxRetryAttemptAndError() {
    JobEntity job = newPendingJob();
    job.setMaxRetries(2);
    job = store().save(job);
    store().claimNextBatch(1, "node-1");

    assertTrue(
        store().markJobFailedTerminal(job.getId(), "retries exhausted", job.getMaxRetries()));

    Optional<JobEntity> terminal = store().findById(job.getId());
    assertTrue(terminal.isPresent());
    assertEquals(JobStatus.FAILED, terminal.get().getStatus());
    assertEquals("retries exhausted", terminal.get().getLastError());
    assertEquals(2, terminal.get().getAttempts());
    assertEquals(2, terminal.get().getMaxRetries());
  }

  @Test
  void claimRespectsScheduledTime() {
    JobEntity futureJob = newPendingJob();
    futureJob.setScheduledTime(Instant.now().plusSeconds(3600));
    store().save(futureJob);

    JobEntity nowJob = store().save(newPendingJob());

    List<JobEntity> claimed = store().claimNextBatch(10, "node-1");
    assertEquals(1, claimed.size());
    assertEquals(nowJob.getId(), claimed.get(0).getId());
  }
}
