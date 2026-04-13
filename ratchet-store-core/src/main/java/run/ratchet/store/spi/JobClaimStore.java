package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import java.util.List;

/** Dialect-specific job claiming operations using SKIP LOCKED and priority boosting. */
@Incubating
public interface JobClaimStore {

  List<JobEntity> claimNextBatch(int limit, String nodeId);

  List<JobClaimDto> claimNextBatchOptimized(int limit, String nodeId);

  List<JobEntity> claimDueRecurring(int limit, String nodeId);
}
