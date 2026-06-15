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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;

/**
 * Base contract for the existence-hiding and principal-scoping guarantees of {@link
 * JobQueryService}. Runs against {@link #deniedQueryService()} — a query service whose
 * authorization policy denies reads and scopes list queries to a non-matching principal. A denied
 * {@code getJobDetail} returns empty exactly as a missing job does (existence is hidden), and a
 * scoped {@code findJobs} returns nothing. Because it needs a read-denying policy, this runs in its
 * own deployment, separate from the permit-all {@link AbstractJobQueryContract}.
 */
public abstract class AbstractJobQueryDenialContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void deniedRead_hidesExistenceOfRealJob() {
    JobQueryService denied = deniedQueryService();
    JobHandle handle = submitTracked();

    assertTrue(
        denied.getJobDetail(handle.id()).isEmpty(),
        "a denied read of an existing job must return empty");
    assertTrue(
        denied.getJobDetail(UUID.randomUUID()).isEmpty(), "an unknown job also returns empty");
    // The two are indistinguishable — existence is hidden, not signalled by a different outcome.
    assertTrue(
        denied.getExecutionHistory(handle.id()).isEmpty(),
        "a denied read must return no execution history for an existing job");
  }

  @Test
  void principalScopedFilter_excludesJobsFromFindJobs() {
    submitTracked();

    assertTrue(
        deniedQueryService().findJobs(JobFilter.builder().build(), 100, 0).items().isEmpty(),
        "filterForPrincipal must scope findJobs so a caller sees no out-of-scope jobs");
  }

  protected abstract RatchetTckRuntime runtime();

  /**
   * A {@link JobQueryService} whose authorization policy denies reads and scopes list queries to a
   * non-matching principal.
   */
  protected abstract JobQueryService deniedQueryService();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }

  private JobHandle submitTracked() {
    JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
    runtime().probe().track(handle);
    return handle;
  }
}
