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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;

/** Base contract tests for {@code JobRetryStore}. */
public abstract class AbstractJobRetryStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupRetryFixture() {
    cleanupStore();
  }

  @Test
  void incrementRetryAttempt_requiresRetryableStatus() {
    var saved = persist(newPendingJob());

    assertEquals(
        -1,
        store().incrementRetryAttempt(saved.getId()),
        "Retry attempts should not increment for non-retryable jobs");

    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    assertEquals(1, store().incrementRetryAttempt(saved.getId()));
  }

  @Test
  void scheduleJobRetry_setsNewTimeAndAttempts() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Instant retryTime = Instant.now().plusSeconds(300);
    boolean retried = store().scheduleJobRetry(saved.getId(), "transient error", retryTime, 1);

    assertTrue(retried, "scheduleJobRetry should succeed for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus(), "Job should be back to PENDING");
  }

  @Test
  void scheduleJobRetry_supportsWaitingSignalTimeouts() {
    var waiting = newPendingJob();
    waiting.setStatus(JobStatus.WAITING);
    waiting.setSignalKey("approval");
    waiting.setSignalTimeout(Instant.now().minusSeconds(1));
    var saved = persist(waiting);

    assertEquals(1, store().incrementRetryAttempt(saved.getId()));

    Instant retryTime = Instant.now().plusSeconds(300);
    boolean retried = store().scheduleJobRetry(saved.getId(), "signal timeout", retryTime, 1);

    assertTrue(retried, "scheduleJobRetry should succeed for a WAITING timeout");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus(), "Job should be back to PENDING");
  }

  @Test
  void scheduleJobRetry_rejectsFailedTerminalRows() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.FAILED, "boom");

    boolean retried =
        store()
            .scheduleJobRetry(saved.getId(), "already terminal", Instant.now().plusSeconds(300), 1);

    assertFalse(retried, "FAILED terminal jobs should not be rescheduled through retry");
    assertEquals(JobStatus.FAILED, store().getJobStatus(saved.getId()));
  }

  @Test
  void resetFailedToPending_transitionsAndResetsMetadata() {
    // Schedule the job an hour out so the ~now assertion below proves the reset actually
    // rewrote scheduled_time; with a ~now starting value the assertion would be vacuous.
    var pending = newPendingJob();
    pending.setScheduledTime(Instant.now().plusSeconds(3600));
    var saved = persist(pending);
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    // Accumulate retry metadata so the reset has something to actually clear; otherwise
    // asserting attempts==0 is vacuous (the row started at 0).
    store().incrementRetryAttempt(saved.getId());
    store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.FAILED, "error");

    boolean reset = store().resetFailedToPending(saved.getId());
    Instant afterReset = Instant.now();

    assertTrue(reset, "resetFailedToPending should succeed for a FAILED job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus(), "status must flip FAILED -> PENDING");
    assertEquals(0, reloaded.getAttempts(), "retry attempts must reset to 0");
    assertNull(reloaded.getLastError(), "last error must be cleared");
    // 5 s of slack covers DB-server-clock vs JVM-clock skew (SQL stores write NOW()/
    // statement_timestamp() while this test reads Instant.now()) without letting a deliberate
    // future reschedule pass for "immediately eligible".
    assertFalse(
        reloaded.getScheduledTime().isAfter(afterReset.plusSeconds(5)),
        "job must be rescheduled to ~now (immediately eligible), not left in the future");
  }

  @Test
  void incrementRetryAttempt_unknownJob_returnsMinusOne() {
    assertEquals(-1, store().incrementRetryAttempt(new UUID(0L, Long.MAX_VALUE)));
  }

  @Test
  void scheduleJobRetry_rejectsNonRetryableOrMissingRows() {
    var pending = persist(newPendingJob());
    Instant retryTime = Instant.now().plusSeconds(300);

    assertFalse(
        store().scheduleJobRetry(pending.getId(), "not running", retryTime, 1),
        "PENDING jobs should not be rescheduled through retry");
    assertFalse(
        store().scheduleJobRetry(new UUID(0L, Long.MAX_VALUE), "missing", retryTime, 1),
        "missing jobs should not be rescheduled through retry");
  }

  @Test
  void resetFailedToPending_rejectsNonFailedOrMissingRows() {
    var pending = persist(newPendingJob());

    assertFalse(
        store().resetFailedToPending(pending.getId()),
        "resetFailedToPending should only reset FAILED jobs");
    assertFalse(store().resetFailedToPending(new UUID(0L, Long.MAX_VALUE)));
  }

  @Test
  void resetFailedToPending_bulkHonorsFilterAndLimit() {
    JobEntity first = failedJob("bulk-recover");
    JobEntity second = failedJob("bulk-recover");
    JobEntity other = failedJob("leave-failed");

    int reset = store().resetFailedToPending(JobFilter.builder().tags("bulk-recover").build(), 1);

    assertEquals(1, reset, "the explicit bulk limit must bound the atomic reset");
    List<JobEntity> matching =
        List.of(
            store().findById(first.getId()).orElseThrow(),
            store().findById(second.getId()).orElseThrow());
    assertEquals(
        1,
        matching.stream().filter(job -> job.getStatus() == JobStatus.PENDING).count(),
        "exactly one matching FAILED job should become PENDING");
    assertEquals(
        1,
        matching.stream().filter(job -> job.getStatus() == JobStatus.FAILED).count(),
        "the other matching job must remain FAILED for a later bounded call");

    JobEntity retried =
        matching.stream()
            .filter(job -> job.getStatus() == JobStatus.PENDING)
            .findFirst()
            .orElseThrow();
    assertEquals(0, retried.getAttempts(), "bulk retry must reset the attempt counter");
    assertNull(retried.getLastError(), "bulk retry must clear the terminal error");
    assertEquals(
        JobStatus.FAILED,
        store().getJobStatus(other.getId()),
        "jobs outside the filter must not be changed");
  }

  @Test
  void resetFailedToPending_bulkIntersectsExplicitStatusesWithFailed() {
    JobEntity failed = failedJob("status-filter");

    int reset =
        store().resetFailedToPending(JobFilter.builder().statuses(JobStatus.PENDING).build(), 10);

    assertEquals(0, reset);
    assertEquals(JobStatus.FAILED, store().getJobStatus(failed.getId()));
  }

  @Test
  void resetFailedToPending_bulkRollsBackWholeSelectionOnBusinessKeyConflict() {
    String tag = "bulk-retry-rollback";
    String businessKey = "bulk-retry-conflict-" + UUID.randomUUID();
    JobEntity safe = failedJob(tag);

    JobEntity conflicted = newPendingJob(tag);
    conflicted.setBusinessKey(businessKey);
    JobEntity savedConflict = persist(conflicted);
    assertTrue(
        store()
            .compareAndSwapStatus(
                savedConflict.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));
    assertTrue(
        store()
            .compareAndSwapStatus(
                savedConflict.getId(), JobStatus.RUNNING, JobStatus.FAILED, "failed owner"));

    JobEntity activeOwner = newPendingJob();
    activeOwner.setBusinessKey(businessKey);
    JobEntity savedOwner = persist(activeOwner);

    assertThrows(
        RatchetTransientStoreException.class,
        () -> store().resetFailedToPending(JobFilter.builder().tags(tag).build(), 10),
        "a business-key conflict is recoverable and must surface as the documented transient type");

    assertEquals(
        JobStatus.FAILED,
        store().getJobStatus(safe.getId()),
        "an unrelated selected job must roll back with the conflicting job");
    assertEquals(JobStatus.FAILED, store().getJobStatus(savedConflict.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(savedOwner.getId()));
  }

  @Test
  void resetFailedToPending_bulkRejectsUnboundedLimits() {
    JobFilter allJobs = JobFilter.builder().build();

    assertThrows(NullPointerException.class, () -> store().resetFailedToPending(null, 1));
    assertThrows(IllegalArgumentException.class, () -> store().resetFailedToPending(allJobs, 0));
    assertThrows(IllegalArgumentException.class, () -> store().resetFailedToPending(allJobs, 1001));
  }

  private JobEntity failedJob(String tag) {
    JobEntity saved = persist(newPendingJob(tag));
    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));
    assertEquals(1, store().incrementRetryAttempt(saved.getId()));
    assertTrue(
        store()
            .compareAndSwapStatus(
                saved.getId(), JobStatus.RUNNING, JobStatus.FAILED, "bulk retry fixture"));
    return saved;
  }
}
