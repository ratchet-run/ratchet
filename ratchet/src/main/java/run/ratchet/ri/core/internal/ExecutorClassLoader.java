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
package run.ratchet.ri.core.internal;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Pins executor tasks to the application loader captured when one runtime starts. */
public final class ExecutorClassLoader {
  private volatile ClassLoader applicationClassLoader;

  /** Captures once, including when executors were resolved before runtime startup. */
  public synchronized void initialize(ClassLoader loader) {
    if (applicationClassLoader == null) {
      applicationClassLoader = Objects.requireNonNull(loader, "applicationClassLoader");
    }
  }

  public ExecutorService wrap(ExecutorService executor) {
    return new ContextExecutor(executor);
  }

  public ScheduledExecutorService wrap(ScheduledExecutorService executor) {
    return new ContextScheduler(executor);
  }

  /** Preserves the registering thread's loader across a container transaction callback. */
  public static Runnable preserveContext(Runnable action) {
    return withLoader(action, Thread.currentThread().getContextClassLoader());
  }

  private static Runnable withLoader(Runnable action, ClassLoader loader) {
    Objects.requireNonNull(action, "action");
    return () -> {
      Thread thread = Thread.currentThread();
      ClassLoader previous = thread.getContextClassLoader();
      thread.setContextClassLoader(loader);
      try {
        action.run();
      } finally {
        thread.setContextClassLoader(previous);
      }
    };
  }

  private Runnable contextual(Runnable command) {
    Objects.requireNonNull(command, "command");
    return () -> {
      ClassLoader loader = applicationClassLoader;
      if (loader == null) command.run();
      else withLoader(command, loader).run();
    };
  }

  private <V> Callable<V> contextual(Callable<V> command) {
    Objects.requireNonNull(command, "command");
    return () -> {
      ClassLoader loader = applicationClassLoader;
      if (loader == null) return command.call();
      Thread thread = Thread.currentThread();
      ClassLoader previous = thread.getContextClassLoader();
      thread.setContextClassLoader(loader);
      try {
        return command.call();
      } finally {
        thread.setContextClassLoader(previous);
      }
    };
  }

  private class ContextExecutor extends AbstractExecutorService {
    private final ExecutorService delegate;

    ContextExecutor(ExecutorService delegate) {
      this.delegate = Objects.requireNonNull(delegate, "executor");
    }

    @Override
    public void execute(Runnable command) {
      delegate.execute(contextual(command));
    }

    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }
  }

  private final class ContextScheduler extends ContextExecutor implements ScheduledExecutorService {
    private final ScheduledExecutorService scheduler;

    ContextScheduler(ScheduledExecutorService scheduler) {
      super(scheduler);
      this.scheduler = scheduler;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return scheduler.schedule(contextual(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit) {
      return scheduler.schedule(contextual(command), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      return scheduler.scheduleAtFixedRate(contextual(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return scheduler.scheduleWithFixedDelay(contextual(command), initialDelay, delay, unit);
    }
  }
}
