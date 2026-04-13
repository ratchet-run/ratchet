package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/** Supplies the thread pools used for job execution and scheduling. */
@Incubating
public interface ExecutorProvider {

  /** Returns the executor used for running jobs. */
  ExecutorService getJobExecutor();

  /** Returns the executor used for delayed and periodic scheduling. */
  ScheduledExecutorService getScheduledExecutor();
}
