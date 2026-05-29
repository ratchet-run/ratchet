package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.ExecutorTargets;
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
  private volatile ExecutorService virtualJobExecutor;

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

  @SuppressWarnings("JavaReflectionMemberAccess")
  private static ExecutorService newVirtualThreadExecutor() {
    try {
      Method factory = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
      return (ExecutorService) factory.invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Standalone virtual executor requires Java 21 or newer", e);
    }
  }

  @Override
  public ExecutorService getJobExecutor() {
    return jobExecutor;
  }

  @Override
  public Optional<ExecutorService> getJobExecutor(String target) {
    if (ExecutorTargets.PLATFORM.equals(target)) {
      return Optional.of(getJobExecutor());
    }
    if (ExecutorTargets.VIRTUAL.equals(target)) {
      return Optional.of(virtualJobExecutor());
    }
    return Optional.empty();
  }

  private ExecutorService virtualJobExecutor() {
    ExecutorService executor = virtualJobExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = virtualJobExecutor;
        if (executor == null) {
          executor = newVirtualThreadExecutor();
          virtualJobExecutor = executor;
        }
      }
    }
    return executor;
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    return scheduledExecutor;
  }

  @PreDestroy
  void shutdown() {
    shutdown(jobExecutor);
    ExecutorService virtualExecutor = virtualJobExecutor;
    if (virtualExecutor != null) {
      shutdown(virtualExecutor);
    }
    shutdown(scheduledExecutor);
  }
}
