package run.ratchet.store.spi;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobExecutionEntity;

/** Execution history tracking operations. */
@Incubating
public interface ExecutionStore {

  int DEFAULT_PAGE_LIMIT = 100;

  /** Saves one execution record. Transaction attribute: {@code REQUIRED}. */
  JobExecutionEntity saveExecution(JobExecutionEntity execution);

  /**
   * Returns the first page of execution records for a job, ordered by attempt ascending.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @deprecated use {@link #findExecutionsByJobId(UUID, int, int)} when callers need to walk more
   *     than the default page.
   */
  @Deprecated(since = "0.1.0", forRemoval = false)
  default List<JobExecutionEntity> findExecutionsByJobId(UUID jobId) {
    return findExecutionsByJobId(jobId, DEFAULT_PAGE_LIMIT, 0);
  }

  /**
   * Returns a page of execution records for a job, ordered by attempt ascending.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobExecutionEntity> findExecutionsByJobId(UUID jobId, int limit, int offset);

  /** Finds the latest execution for one job. Transaction attribute: {@code SUPPORTS}. */
  Optional<JobExecutionEntity> findLatestExecution(UUID jobId);

  /** Counts execution attempts for one job. Transaction attribute: {@code SUPPORTS}. */
  int countExecutionAttempts(UUID jobId);
}
