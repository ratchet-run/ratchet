package run.ratchet.spi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import run.ratchet.api.Incubating;

/** Supplies the thread pools used for job execution and scheduling. */
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
