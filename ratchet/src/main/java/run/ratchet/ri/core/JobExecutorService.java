package run.ratchet.ri.core;

import java.time.Duration;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;

/**
 * SPI for executing jobs on the configured executor. Default implementation: {@link
 * run.ratchet.ri.core.internal.DefaultJobExecutorService}.
 *
 * @apiNote Framework SPI consumed by ri.core.JobSubmissionService / JobExecutionCoordinator and by
 *     ratchet-testsuite integration tests. Applications must not implement this interface.
 */
public interface JobExecutorService {

  ExecutionResult execute(JobEntity job, String poolName);

  ExecutionResult execute(JobClaimDto claim, String poolName);

  /**
   * Non-destructive drain: blocks until all currently-executing jobs reach a terminal state, or
   * until {@code timeout} elapses. Does NOT cancel running tasks. Pair with {@link
   * DrainController#setDraining(boolean)} on entry to prevent new work from arriving.
   *
   * @return {@code true} if the executor became idle within the timeout; {@code false} otherwise
   */
  boolean awaitIdle(Duration timeout) throws InterruptedException;

  int shutdownActiveExecutions();
}
