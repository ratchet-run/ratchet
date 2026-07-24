/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.function.Consumer;
import java.util.function.Supplier;
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

  // Pools are created lazily on first use, not in field initializers. Eager creation makes the
  // bean construction start OS threads; under a build-time-CDI runtime (Quarkus native) the bean is
  // instantiated during image build and a live thread would be captured into the image heap
  // ("Detected a started Thread in the image heap"). Lazy creation keeps construction inert.
  private volatile ExecutorService jobExecutor;
  private volatile ScheduledExecutorService scheduledExecutor;
  private volatile ExecutorService virtualJobExecutor;
  private volatile boolean closed;

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
    return lazyInit(
        () -> jobExecutor,
        v -> jobExecutor = v,
        () -> Executors.newCachedThreadPool(namedThreadFactory("ratchet-standalone-job")));
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
    return lazyInit(
        () -> virtualJobExecutor,
        v -> virtualJobExecutor = v,
        StandaloneExecutorProvider::newVirtualThreadExecutor);
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    return lazyInit(
        () -> scheduledExecutor,
        v -> scheduledExecutor = v,
        () ->
            Executors.newScheduledThreadPool(2, namedThreadFactory("ratchet-standalone-scheduler")));
  }

  /**
   * Double-checked-locking memoizer shared by the three pool getters: a volatile read outside the
   * lock avoids synchronizing on every call after first init, {@code ensureOpen()} runs only on
   * the construction path so a pool never gets created after {@link #shutdown()}.
   */
  private <T> T lazyInit(Supplier<T> getter, Consumer<T> setter, Supplier<T> factory) {
    T value = getter.get();
    if (value == null) {
      synchronized (this) {
        value = getter.get();
        if (value == null) {
          ensureOpen();
          value = factory.get();
          setter.accept(value);
        }
      }
    }
    return value;
  }

  @PreDestroy
  synchronized void shutdown() {
    // Set before shutting the pools down so a concurrent lazy getter (which locks on this) cannot
    // resurrect a pool after @PreDestroy and leak its threads.
    closed = true;
    shutdownIfPresent(jobExecutor);
    shutdownIfPresent(virtualJobExecutor);
    shutdownIfPresent(scheduledExecutor);
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("StandaloneExecutorProvider is shut down");
    }
  }

  private static void shutdownIfPresent(ExecutorService executor) {
    if (executor != null) {
      shutdown(executor);
    }
  }
}
