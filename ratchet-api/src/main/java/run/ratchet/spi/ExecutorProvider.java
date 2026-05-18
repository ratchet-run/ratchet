package run.ratchet.spi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import run.ratchet.api.Incubating;

/**
 * Supplies the thread pools used for job execution and scheduling.
 *
 * <p>Each accessor must return a stable executor instance for the provider lifecycle. Consumers may
 * retain returned references for cancellation, delayed scheduling, and lifecycle coordination.
 *
 * <p>Executor ownership is implementation-specific. Managed-runtime providers must not shut down
 * container-owned executors, and callers must not invoke shutdown methods on executors they did not
 * create. Standalone providers that create their own pools are responsible for disposing them
 * during application shutdown.
 */
@Incubating
public interface ExecutorProvider {

  /**
   * Returns the executor used for job payload execution.
   *
   * @return job executor; never {@code null}
   */
  ExecutorService getJobExecutor();

  /**
   * Returns the scheduler used for timers, watchdogs, and delayed maintenance work.
   *
   * @return scheduled executor; never {@code null}
   */
  ScheduledExecutorService getScheduledExecutor();
}
