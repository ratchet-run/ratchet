package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.spi.ExecutorProvider;

/**
 * Opt-in unmanaged executor provider for plain CDI, demos, and tests.
 *
 * <p>The default production provider uses Jakarta Concurrency managed executors. Enable this bean
 * only when running outside a Jakarta Concurrency-capable runtime.
 */
@Alternative
@ApplicationScoped
public class StandaloneExecutorProvider implements ExecutorProvider {

  private final ExecutorService jobExecutor =
      Executors.newCachedThreadPool(namedThreadFactory("ratchet-standalone-job"));
  private final ScheduledExecutorService scheduledExecutor =
      Executors.newScheduledThreadPool(2, namedThreadFactory("ratchet-standalone-scheduler"));

  private static void shutdown(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory namedThreadFactory(String prefix) {
    AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  @Override
  public ExecutorService getJobExecutor() {
    return jobExecutor;
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    return scheduledExecutor;
  }

  @PreDestroy
  void shutdown() {
    shutdown(jobExecutor);
    shutdown(scheduledExecutor);
  }
}
