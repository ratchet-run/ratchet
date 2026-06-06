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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSummary;

/**
 * Base contract for the positive {@link JobQueryService} read surface: findJobs, getJobDetail,
 * getExecutionHistory, and getRecurringMasters round-trip a submitted job under a permit-all
 * authorization policy. The existence-hiding and principal-scoping guarantees live in {@link
 * AbstractJobQueryDenialContract}, which needs a read-denying policy and so runs in its own
 * deployment.
 */
public abstract class AbstractJobQueryContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void getJobDetail_returnsSubmittedJob() {
    JobHandle handle = submitTracked();

    Optional<UUID> detailId =
        queryService().getJobDetail(handle.id()).map(detail -> detail.summary().id());
    assertTrue(detailId.isPresent(), "getJobDetail must return a submitted job");
    assertEquals(handle.id(), detailId.get(), "the detail summary must carry the job id");
  }

  @Test
  void getJobDetail_unknownId_returnsEmpty() {
    assertTrue(
        queryService().getJobDetail(UUID.randomUUID()).isEmpty(),
        "getJobDetail for an unknown id must return empty");
  }

  @Test
  void findJobs_includesSubmittedJob() {
    JobHandle handle = submitTracked();

    boolean found =
        queryService().findJobs(JobFilter.builder().build(), 100, 0).items().stream()
            .anyMatch(summary -> handle.id().equals(summary.id()));
    assertTrue(found, "findJobs must surface a submitted job under a permit-all policy");
  }

  @Test
  void getExecutionHistory_recordsAttemptForCompletedJob() {
    JobHandle handle = submitTracked();
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()), "setup job must complete");

    assertFalse(
        queryService().getExecutionHistory(handle.id()).isEmpty(),
        "getExecutionHistory must record at least one attempt for a completed job");
  }

  @Test
  void getRecurringMasters_roundTripsIncludingPausedState() {
    // Fires once a year, so it never spawns a child during the test.
    JobHandle master =
        runtime().scheduler().scheduleRecurringUtc("0 0 0 1 1 ?", TckJobs::noop).submit();

    assertTrue(
        recurringMaster(master.id()).isPresent(),
        "a registered recurring master must be visible to getRecurringMasters");

    assertTrue(
        runtime().scheduler().pauseJob(master.id()), "pausing the recurring master succeeds");

    Optional<JobSummary> paused = recurringMaster(master.id());
    assertTrue(
        paused.isPresent(), "a paused recurring master stays visible to getRecurringMasters");
    assertEquals(
        JobStatus.PAUSED, paused.get().status(), "the master must report PAUSED after pause");
  }

  protected abstract RatchetTckRuntime runtime();

  /** The permit-all {@link JobQueryService} under test. */
  protected abstract JobQueryService queryService();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }

  private JobHandle submitTracked() {
    JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
    runtime().probe().track(handle);
    return handle;
  }

  private Optional<JobSummary> recurringMaster(UUID masterId) {
    return queryService().getRecurringMasters().items().stream()
        .filter(summary -> masterId.equals(summary.id()))
        .findFirst();
  }
}
