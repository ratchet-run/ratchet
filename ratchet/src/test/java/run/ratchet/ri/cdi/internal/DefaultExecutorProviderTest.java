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
package run.ratchet.ri.cdi.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.naming.InitialContext;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.internal.ExecutorProviderClosedException;

class DefaultExecutorProviderTest {

  @Test
  void releasePoolUsesConfiguredCoreSize() throws Exception {
    InitialContext context = mock(InitialContext.class);
    RatchetOptions options =
        RatchetOptions.builder().execution(execution -> execution.releaseThreads(5)).build();
    when(context.lookup(options.execution().threadFactoryJndi()))
        .thenReturn((ThreadFactory) Thread::new);
    DefaultExecutorProvider provider = new TestExecutorProvider(context, options);
    try {
      provider.getReleaseExecutor();
      Field pool = DefaultExecutorProvider.class.getDeclaredField("releasePool");
      pool.setAccessible(true);
      assertEquals(5, ((ScheduledThreadPoolExecutor) pool.get(provider)).getCorePoolSize());
    } finally {
      provider.shutdown();
    }
  }

  @Test
  void managedReleasePoolUsesFactoryAndDropsDelayedWork() throws Exception {
    AtomicInteger threads = new AtomicInteger();
    InitialContext context = mock(InitialContext.class);
    ScheduledExecutorService containerScheduler = mock(ScheduledExecutorService.class);
    when(context.lookup(DefaultExecutorProvider.SCHEDULED_EXECUTOR_JNDI))
        .thenReturn(containerScheduler);
    DefaultExecutorProvider provider =
        new DefaultExecutorProvider(
            runnable -> {
              threads.incrementAndGet();
              return new Thread(runnable);
            }) {
          @Override
          InitialContext newInitialContext() {
            return context;
          }
        };
    var releases = provider.getReleaseExecutor();
    CountDownLatch delayed = new CountDownLatch(1);
    try {
      assertNotSame(provider.getScheduledExecutor(), releases);
      assertNotSame(containerScheduler, releases);
      assertTrue(
          releases
              .submit(() -> Thread.currentThread().getName())
              .get(5, TimeUnit.SECONDS)
              .startsWith("ratchet-release-"));
      assertTrue(threads.get() > 0);
      releases.schedule(delayed::countDown, 1, TimeUnit.DAYS);
    } finally {
      provider.shutdown();
    }
    verify(containerScheduler, never()).shutdown();
    verify(containerScheduler, never()).shutdownNow();
    assertTrue(releases.awaitTermination(5, TimeUnit.SECONDS));
    assertFalse(delayed.await(100, TimeUnit.MILLISECONDS));
    assertThrows(IllegalStateException.class, provider::getReleaseExecutor);
  }

  @Test
  void shutdownLetsAnInFlightReleaseFinishWithoutInterrupt() throws Exception {
    DefaultExecutorProvider provider = new DefaultExecutorProvider(Thread::new);
    var releases = provider.getReleaseExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<RuntimeException> reschedule = new AtomicReference<>();
    releases.execute(
        () -> {
          started.countDown();
          try {
            finish.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            interrupted.set(true);
            return;
          }
          // A release that fails again asks for the executor to reschedule itself.
          try {
            provider.getReleaseExecutor();
          } catch (RuntimeException e) {
            reschedule.set(e);
          }
        });
    assertTrue(started.await(5, TimeUnit.SECONDS));
    Thread stopper = new Thread(provider::shutdown);
    stopper.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!releases.isShutdown() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(releases.isShutdown());

    finish.countDown();
    stopper.join(5_000);

    assertFalse(stopper.isAlive());
    assertFalse(interrupted.get());
    assertInstanceOf(ExecutorProviderClosedException.class, reschedule.get());
    assertTrue(releases.awaitTermination(1, TimeUnit.SECONDS));
  }

  @Test
  void closedProviderCannotCreateReleasePool() {
    DefaultExecutorProvider provider = new DefaultExecutorProvider(Thread::new);
    provider.shutdown();
    assertThrows(IllegalStateException.class, provider::getReleaseExecutor);
  }

  @Test
  void missingManagedFactoryNamesWorkerOption() {
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(execution -> execution.threadFactoryJndi("java:missing/releaseFactory"))
            .build();
    DefaultExecutorProvider provider = new DefaultExecutorProvider(options);
    try {
      assertTrue(
          assertThrows(IllegalStateException.class, provider::getReleaseExecutor)
              .getMessage()
              .contains("ratchet.worker.thread-factory-jndi"));
    } finally {
      provider.shutdown();
    }
  }

  @Test
  void initResolvesManagedExecutorsBeforeFirstGetter() throws Exception {
    InitialContext context = mock(InitialContext.class);
    ExecutorService jobExecutor = mock(ExecutorService.class);
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    when(context.lookup(DefaultExecutorProvider.JOB_EXECUTOR_JNDI)).thenReturn(jobExecutor);
    when(context.lookup(DefaultExecutorProvider.SCHEDULED_EXECUTOR_JNDI))
        .thenReturn(scheduledExecutor);
    String threadFactoryName = RatchetOptions.defaults().execution().threadFactoryJndi();
    when(context.lookup(threadFactoryName)).thenReturn(mock(ThreadFactory.class));

    DefaultExecutorProvider provider = new TestExecutorProvider(context);

    provider.init();

    verify(context).lookup(DefaultExecutorProvider.JOB_EXECUTOR_JNDI);
    verify(context).lookup(DefaultExecutorProvider.SCHEDULED_EXECUTOR_JNDI);
    verify(context).lookup(threadFactoryName);
    assertNotSame(jobExecutor, provider.getJobExecutor());
    assertNotSame(scheduledExecutor, provider.getScheduledExecutor());
    assertSame(provider.getJobExecutor(), provider.getJobExecutor());
    assertSame(provider.getScheduledExecutor(), provider.getScheduledExecutor());
    provider.getJobExecutor().execute(() -> {});
    verify(jobExecutor).execute(any(Runnable.class));
  }

  @Test
  void resolvesConfiguredExecutorJndiNamesFromOptions() throws Exception {
    String jobName = "java:app/concurrent/RatchetVirtualExecutor";
    String scheduledName = "java:app/concurrent/RatchetVirtualScheduledExecutor";
    String threadFactoryName = "java:app/concurrent/RatchetThreadFactory";
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(
                execution ->
                    execution
                        .jobExecutorJndi(jobName)
                        .scheduledExecutorJndi(scheduledName)
                        .threadFactoryJndi(threadFactoryName))
            .build();
    InitialContext context = mock(InitialContext.class);
    ExecutorService jobExecutor = mock(ExecutorService.class);
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    when(context.lookup(jobName)).thenReturn(jobExecutor);
    when(context.lookup(scheduledName)).thenReturn(scheduledExecutor);
    when(context.lookup(threadFactoryName)).thenReturn(mock(ThreadFactory.class));

    DefaultExecutorProvider provider = new TestExecutorProvider(context, options);

    provider.init();

    verify(context).lookup(jobName);
    verify(context).lookup(scheduledName);
    verify(context).lookup(threadFactoryName);
    assertNotSame(jobExecutor, provider.getJobExecutor());
    assertNotSame(scheduledExecutor, provider.getScheduledExecutor());
    assertSame(provider.getJobExecutor(), provider.getJobExecutor());
    assertSame(provider.getScheduledExecutor(), provider.getScheduledExecutor());
    provider.getJobExecutor().execute(() -> {});
    verify(jobExecutor).execute(any(Runnable.class));
  }

  private static final class TestExecutorProvider extends DefaultExecutorProvider {
    private final InitialContext context;

    private TestExecutorProvider(InitialContext context) {
      this.context = context;
    }

    private TestExecutorProvider(InitialContext context, RatchetOptions options) {
      super(options);
      this.context = context;
    }

    @Override
    InitialContext newInitialContext() {
      return context;
    }
  }
}
