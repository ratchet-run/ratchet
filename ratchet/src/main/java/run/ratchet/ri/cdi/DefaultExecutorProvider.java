package run.ratchet.ri.cdi;

import run.ratchet.spi.ExecutorProvider;
import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default {@link ExecutorProvider} that delegates to the container's managed executor services.
 *
 * <p>Uses Jakarta Concurrency {@code @Resource} injection to obtain managed executors from the
 * application server. Users can override by providing their own {@code @ApplicationScoped
 * ExecutorProvider} bean.
 */
@ApplicationScoped
public class DefaultExecutorProvider implements ExecutorProvider {

  @Resource(lookup = "java:comp/DefaultManagedExecutorService")
  private ExecutorService executorService;

  @Resource(lookup = "java:comp/DefaultManagedScheduledExecutorService")
  private ScheduledExecutorService scheduledExecutorService;

  @Override
  public ExecutorService getJobExecutor() {
    return executorService;
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    return scheduledExecutorService;
  }
}
