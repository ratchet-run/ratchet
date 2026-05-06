package run.ratchet.store.spi;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobExecutionEntity;

/** Execution history tracking operations. */
@Incubating
public interface ExecutionStore {

  JobExecutionEntity saveExecution(JobExecutionEntity execution);

  List<JobExecutionEntity> findExecutionsByJobId(UUID jobId);

  Optional<JobExecutionEntity> findLatestExecution(UUID jobId);

  int countExecutionAttempts(UUID jobId);
}
