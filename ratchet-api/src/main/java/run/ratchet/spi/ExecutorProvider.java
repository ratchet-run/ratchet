package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/** Supplies the thread pools used for job execution and scheduling. */
@Incubating
public interface ExecutorProvider {

  ExecutorService getJobExecutor();

  ScheduledExecutorService getScheduledExecutor();
}
