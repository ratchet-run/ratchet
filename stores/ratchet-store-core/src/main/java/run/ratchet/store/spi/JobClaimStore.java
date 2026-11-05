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
package run.ratchet.store.spi;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Dialect-specific job claiming operations using SKIP LOCKED and priority boosting. */
@Incubating
public interface JobClaimStore {

  /**
   * Returns the status and claim sequence of the job's live row. Empty when the job does not exist
   * or has finished (SUCCEEDED, FAILED, CANCELED). Terminal jobs are empty because the stored
   * sequence of a SUCCEEDED or CANCELED job is not kept current.
   *
   * <p>Reads the latest committed state outside the caller's transaction. Takes no locks and does
   * not load, decrypt or deserialize payload, params or callbacks. Transaction attribute: {@code
   * REQUIRES_NEW}.
   *
   * <p>Only a claim ({@code claimNextBatch*}) or {@code tryPickUpJob} starts a new attempt; both
   * increase the sequence. Other writes setting RUNNING ({@code updateJobStatus}, {@code
   * compareAndSwapStatus}, {@code save}, {@code transitionFromPaused*}) keep the sequence and
   * continue the current claim. The runtime never uses them to start an attempt. Releases ({@code
   * resetRunningJob(s)} and orphan resets) set PENDING and keep the sequence.
   */
  Optional<ClaimState> currentClaim(UUID jobId);

  /**
   * Claims due one-shot jobs with SKIP LOCKED semantics. Transaction attribute: {@code REQUIRED}.
   *
   * @param limit maximum number of jobs to claim in this batch; must be positive
   * @param nodeId stable identity of the claiming node; never {@code null} or blank
   * @param tagFilter node-tag affinity filter to apply during claim; never {@code null} (use {@link
   *     NodeTagFilter#NONE} to disable filtering)
   * @return claimed job entities, never {@code null}; may be empty when nothing is due
   */
  List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter);

  /**
   * Claims due jobs and returns lightweight claim rows. Transaction attribute: {@code REQUIRED}.
   *
   * @param jobType internal execution type to restrict the claim to; never {@code null}
   * @param limit maximum number of jobs to claim in this batch; must be positive
   * @param nodeId stable identity of the claiming node; never {@code null} or blank
   * @param tagFilter node-tag affinity filter to apply during claim; never {@code null} (use {@link
   *     NodeTagFilter#NONE} to disable filtering)
   * @return claim DTOs (metadata-only, no payload), never {@code null}; may be empty
   */
  List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType,
      int limit,
      String nodeId,
      NodeTagFilter tagFilter,
      ExecutionTargetFilter executionTargetFilter);

  /** Backward-compatible overload that disables tag-affinity filtering. */
  default List<JobEntity> claimNextBatch(int limit, String nodeId) {
    return claimNextBatch(limit, nodeId, NodeTagFilter.NONE);
  }

  /** Backward-compatible overload that disables tag-affinity filtering. */
  default List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId, NodeTagFilter tagFilter) {
    return claimNextBatchOptimized(jobType, limit, nodeId, tagFilter, ExecutionTargetFilter.any());
  }

  default List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    return claimNextBatchOptimized(jobType, limit, nodeId, NodeTagFilter.NONE);
  }
}
