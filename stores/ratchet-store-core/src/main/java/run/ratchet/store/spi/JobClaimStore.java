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
import run.ratchet.api.Incubating;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Dialect-specific job claiming operations using SKIP LOCKED and priority boosting. */
@Incubating
public interface JobClaimStore {

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
