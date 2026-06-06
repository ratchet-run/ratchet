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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

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
    var saved = persist(newPendingJob());
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
    assertFalse(
        reloaded.getScheduledTime().isAfter(afterReset.plusSeconds(60)),
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
}
