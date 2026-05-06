package run.ratchet.spi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import run.ratchet.api.Incubating;

/** Supplies the thread pools used for job execution and scheduling. */
@Incubating
public interface ExecutorProvider {

  ExecutorService getJobExecutor();

  ScheduledExecutorService getScheduledExecutor();
}
