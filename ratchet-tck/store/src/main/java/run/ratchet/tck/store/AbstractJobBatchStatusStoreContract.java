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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code JobBatchStatusStore}. */
public abstract class AbstractJobBatchStatusStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupBatchStatusFixture() {
    cleanupStore();
  }

  @Test
  void compareAndSwapStatus_updatesExpectedState() {
    var saved = persist(newPendingJob());

    boolean updated =
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    assertTrue(updated, "Pending job should transition to RUNNING");
    assertEquals(JobStatus.RUNNING, store().getJobStatus(saved.getId()));
  }

  @Test
  void compareAndSwapStatus_failsOnStatusMismatch() {
    var saved = persist(newPendingJob());

    boolean updated =
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null);

    assertFalse(updated, "CAS from wrong expected status should return false");
    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(saved.getId()),
        "Status should remain PENDING after failed CAS");
  }

  @Test
  void compareAndSwapStatus_terminalExpected_throws() {
    // A terminal `expected` is caller misuse. Every store rejects it with IllegalArgumentException
    // rather than silently returning false, which a caller could not distinguish from a lost race.
    var saved = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            store()
                .compareAndSwapStatus(saved.getId(), JobStatus.SUCCEEDED, JobStatus.PENDING, null));
  }

  @Test
  void compareAndSwapStatus_concurrent_atMostOneSucceeds() {
    var saved = persist(newPendingJob());
    UUID id = saved.getId();

    AtomicInteger successCount = new AtomicInteger();

    ConcurrentTestRunner.runAll(
        Duration.ofSeconds(10),
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        });

    assertEquals(
        1, successCount.get(), "exactly one CAS should succeed; got " + successCount.get());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(id), "Job should be RUNNING after CAS");
  }

  @Test
  void tryPickUpJob_setsStatusAndPickedBy() {
    var saved = persist(newPendingJob());

    boolean picked = store().tryPickUpJob(saved.getId(), "node-1");

    assertTrue(picked, "tryPickUpJob should succeed on a PENDING job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, reloaded.getStatus());
    assertEquals("node-1", reloaded.getPickedBy());
  }

  @Test
  void tryPickUpJob_failsOnAlreadyRunning() {
    var saved = persist(newPendingJob());
    store().tryPickUpJob(saved.getId(), "node-1");

    boolean secondPick = store().tryPickUpJob(saved.getId(), "node-2");

    assertFalse(secondPick, "tryPickUpJob should fail on an already-running job");
  }

  @Test
  void updateJobStatus_updatesLiveStatusAndError() {
    var saved = persist(newPendingJob());

    store().updateJobStatus(saved.getId(), JobStatus.PAUSED, "manual pause");

    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PAUSED, reloaded.getStatus());
    assertEquals("manual pause", reloaded.getLastError());
  }

  @Test
  void updateJobStatus_terminalTarget_terminalizesRunningJob() {
    // A terminal target routes through the guarded terminal transition on every store, so a RUNNING
    // job becomes SUCCEEDED. Mongo must route here too rather than blindly setting the status
    // field.
    var running = runningJob("node-1");

    store().updateJobStatus(running.getId(), JobStatus.SUCCEEDED, null);

    assertEquals(JobStatus.SUCCEEDED, store().getJobStatus(running.getId()));
  }

  @Test
  void updateJobStatus_terminalTarget_nonRunningJob_isNoOp() {
    // Because terminal targets route through the RUNNING-guarded terminal methods, a non-RUNNING
    // job
    // is left untouched on every store. The Mongo bug flipped a PENDING job straight to SUCCEEDED;
    // the SQL stores leave it PENDING. All three must agree.
    var pending = persist(newPendingJob());

    store().updateJobStatus(pending.getId(), JobStatus.SUCCEEDED, null);

    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(pending.getId()),
        "a non-RUNNING job must not be flipped straight to a terminal status");
  }

  @Test
  void resetRunningJob_reclaimsMatchingNodeOnly() {
    var matching = runningJob("node-1");
    var wrongNode = runningJob("node-2");

    boolean reset = store().resetRunningJob(matching.getId(), "node-1");
    boolean wrongNodeReset = store().resetRunningJob(wrongNode.getId(), "node-1");

    assertTrue(reset, "Matching RUNNING row should be reset");
    assertFalse(wrongNodeReset, "RUNNING row owned by another node should not be reset");

    var resetJob = store().findById(matching.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, resetJob.getStatus());
    assertNull(resetJob.getPickedBy());
    assertNull(resetJob.getPickedAt());

    var preserved = store().findById(wrongNode.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, preserved.getStatus());
    assertEquals("node-2", preserved.getPickedBy());
  }

  @Test
  void resetRunningJobs_reclaimsAllRowsForNode() {
    var first = runningJob("node-1");
    var second = runningJob("node-1");
    var otherNode = runningJob("node-2");

    int reset = store().resetRunningJobs("node-1");

    assertEquals(2, reset, "Only rows owned by the requested node should be reset");
    assertEquals(JobStatus.PENDING, store().getJobStatus(first.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(second.getId()));
    assertEquals(JobStatus.RUNNING, store().getJobStatus(otherNode.getId()));
  }

  private JobEntity runningJob(String nodeId) {
    var job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), nodeId));
    return store().findById(job.getId()).orElseThrow();
  }
}
