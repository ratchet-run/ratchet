package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobExecutionEntity;
import java.util.List;
import java.util.Optional;

/** Execution history tracking operations. */
@Incubating
public interface ExecutionStore {

  /** Persists one execution-attempt record. */
  JobExecutionEntity saveExecution(JobExecutionEntity execution);

  /** Lists all execution attempts recorded for a job in store-defined order. */
  List<JobExecutionEntity> findExecutionsByJobId(long jobId);

  /** Returns the most recent execution attempt for a job when one exists. */
  Optional<JobExecutionEntity> findLatestExecution(long jobId);

  /** Counts how many execution attempts have been recorded for a job. */
  int countExecutionAttempts(long jobId);
}
