package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import java.util.List;

/** Dialect-specific job claiming operations using SKIP LOCKED and priority boosting. */
@Incubating
public interface JobClaimStore {

  /** Claims due non-recurring jobs for execution and returns hydrated entities. */
  List<JobEntity> claimNextBatch(int limit, String nodeId);

  /** Claims due non-recurring jobs and returns lightweight claim DTOs for hot polling paths. */
  List<JobClaimDto> claimNextBatchOptimized(int limit, String nodeId);

  /** Claims due recurring master jobs whose next fire time has arrived. */
  List<JobEntity> claimDueRecurring(int limit, String nodeId);
}
