package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides executors for job execution and scheduling, abstracting the underlying executor service
 * implementations. This service is intended to be used as a source of thread pools for asynchronous
 * job processing and scheduled tasks.
 *
 * <p>Users may supply their own implementation of this interface, or rely on a default
 * implementation provided by the runtime. Custom implementations can be used to integrate with
 * specific concurrency management requirements or to customize thread pool behavior.
 *
 * <p>Marked as {@link Incubating}, indicating that this API may evolve in future releases and is
 * not guaranteed to maintain backward compatibility.
 */
@Incubating
public interface ExecutorProvider {

  /**
   * Retrieves an {@link ExecutorService} for executing asynchronous jobs.
   *
   * @return an {@link ExecutorService} instance that can be used to submit and manage job execution
   *     tasks
   */
  ExecutorService getJobExecutor();

  /**
   * Provides a {@link ScheduledExecutorService} for executing scheduled tasks.
   *
   * @return a {@link ScheduledExecutorService} instance that can be used for scheduling tasks to
   *     run after a delay or periodically
   */
  ScheduledExecutorService getScheduledExecutor();
}
