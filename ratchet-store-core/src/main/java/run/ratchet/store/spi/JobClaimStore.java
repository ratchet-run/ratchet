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
   */
  List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter);

  /**
   * Claims due jobs and returns lightweight claim rows. Transaction attribute: {@code REQUIRED}.
   */
  List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId, NodeTagFilter tagFilter);

  /** Claims due recurring masters. Transaction attribute: {@code REQUIRED}. */
  List<JobEntity> claimDueRecurring(int limit, String nodeId, NodeTagFilter tagFilter);

  default List<JobEntity> claimNextBatch(int limit, String nodeId) {
    return claimNextBatch(limit, nodeId, NodeTagFilter.NONE);
  }

  default List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    return claimNextBatchOptimized(jobType, limit, nodeId, NodeTagFilter.NONE);
  }

  default List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    return claimDueRecurring(limit, nodeId, NodeTagFilter.NONE);
  }
}
