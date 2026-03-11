package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/** Provides executor services for job execution and scheduling. */
@Incubating
public interface ExecutorProvider {

  ExecutorService getJobExecutor();

  ScheduledExecutorService getScheduledExecutor();
}
