package run.ratchet.store.spi;

import run.ratchet.store.entity.JobExecutionEntity;
import java.util.List;
import java.util.Optional;

/** Execution history tracking operations. */
public interface ExecutionStore {

  JobExecutionEntity saveExecution(JobExecutionEntity execution);

  List<JobExecutionEntity> findExecutionsByJobId(long jobId);

  Optional<JobExecutionEntity> findLatestExecution(long jobId);

  int countExecutionAttempts(long jobId);
}
