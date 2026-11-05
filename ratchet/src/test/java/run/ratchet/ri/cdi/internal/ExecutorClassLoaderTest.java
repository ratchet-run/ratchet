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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.net.URL;
import java.net.URLClassLoader;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.naming.InitialContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;
import run.ratchet.ri.core.*;
import run.ratchet.ri.core.internal.*;
import run.ratchet.spi.*;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;

public class ExecutorClassLoaderTest {
  private static final AtomicReference<AtomicReference<ClassLoader>> OBSERVED =
      new AtomicReference<>();

  private static final AtomicReference<ClassLoader> FAIL_WITH_LOADER = new AtomicReference<>();
  private static final AtomicInteger INVOCATIONS = new AtomicInteger();

  public static void jobBody() {
    OBSERVED.get().set(Thread.currentThread().getContextClassLoader());
    INVOCATIONS.incrementAndGet();
    ClassLoader failingLoader = FAIL_WITH_LOADER.getAndSet(null);
    if (failingLoader != null) {
      Thread.currentThread().setContextClassLoader(failingLoader);
      throw new IllegalStateException("retry this attempt");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void jobTaskResolvesApplicationClassAndRestoresWorkerLoader(boolean managed) throws Exception {
    try (Fixture fixture = new Fixture(managed)) {
      fixture.start();
      JobRunner runner = new JobRunner(fixture.provider);
      AtomicReference<ClassLoader> observed = new AtomicReference<>();
      fixture.foreign(
          () -> {
            runner.execute(observed).future().get(5, TimeUnit.SECONDS);
            return null;
          });
      assertSame(fixture.application, observed.get(), "job body must see startup loader");
      fixture.assertRestored();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void crossNodeWakeupPollCycleUsesStartupLoader(boolean managed) throws Exception {
    try (Fixture fixture = new Fixture(managed)) {
      fixture.start();
      PollerCycleExecutor cycles = mock(PollerCycleExecutor.class);
      CountDownLatch polled = new CountDownLatch(1);
      AtomicReference<ClassLoader> observed = new AtomicReference<>();
      when(cycles.tick())
          .thenAnswer(
              invocation -> {
                observed.set(Thread.currentThread().getContextClassLoader());
                Class.forName(ExecutorClassLoaderTest.class.getName(), true, observed.get());
                polled.countDown();
                return 60_000L;
              });
      DefaultPollerScheduler scheduler = new DefaultPollerScheduler(fixture.provider, cycles);
      try {
        fixture.foreign(
            () -> {
              scheduler.start();
              return null;
            });
        assertTrue(polled.await(5, TimeUnit.SECONDS));
        observed.set(null);
        CountDownLatch awakened = new CountDownLatch(1);
        when(cycles.tick())
            .thenAnswer(
                invocation -> {
                  observed.set(Thread.currentThread().getContextClassLoader());
                  awakened.countDown();
                  return 60_000L;
                });
        fixture.foreign(
            () -> {
              scheduler.wakeup();
              return null;
            });
        assertTrue(awakened.await(5, TimeUnit.SECONDS));
        assertSame(fixture.application, observed.get());
      } finally {
        scheduler.stop();
      }
      fixture.assertRestored();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retryCallbackFromForeignThreadRunsRetriedJobWithStartupLoader(boolean managed)
      throws Exception {
    try (Fixture fixture = new Fixture(managed)) {
      fixture.start();
      JobRunner runner = new JobRunner(fixture.provider);
      AtomicReference<ClassLoader> observed = new AtomicReference<>();
      CountDownLatch retried = new CountDownLatch(1);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      PollerCycleExecutor cycles = mock(PollerCycleExecutor.class);
      DefaultPollerScheduler scheduler = new DefaultPollerScheduler(fixture.provider, cycles);
      // The initial cycle has no ready retry; make the next cycle execute the retried job.
      when(cycles.tick()).thenReturn(60_000L);
      scheduler.start();
      fixture.provider.getScheduledExecutor().submit(() -> {}).get(5, TimeUnit.SECONDS);
      when(cycles.tick())
          .thenAnswer(
              invocation -> {
                try {
                  runner.execute(observed).future().get(5, TimeUnit.SECONDS);
                } catch (Throwable t) {
                  failure.set(t);
                } finally {
                  retried.countDown();
                }
                return 60_000L;
              });
      ExecutionObserver observer =
          new ExecutionObserver(null, null, null, null, fixture.provider, scheduler::wakeup);
      try {
        fixture.foreign(
            () -> {
              observer.scheduleDelayedJobReadyCallback(1);
              return null;
            });
        assertTrue(retried.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertSame(fixture.application, observed.get());
      } finally {
        scheduler.stop();
      }
      fixture.assertRestored();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedJobSchedulesRetryFromForeignLoaderAndSucceeds(boolean managed) throws Exception {
    try (Fixture fixture = new Fixture(managed)) {
      fixture.start();
      AtomicReference<ClassLoader> observed = new AtomicReference<>();
      AtomicReference<JobRunner> runner = new AtomicReference<>();
      CountDownLatch initialPoll = new CountDownLatch(1);
      CountDownLatch retried = new CountDownLatch(1);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      PollerCycleExecutor cycles = mock(PollerCycleExecutor.class);
      when(cycles.tick())
          .thenAnswer(
              invocation -> {
                initialPoll.countDown();
                return 60_000L;
              });
      DefaultPollerScheduler scheduler = new DefaultPollerScheduler(fixture.provider, cycles);
      scheduler.start();
      try {
        assertTrue(initialPoll.await(5, TimeUnit.SECONDS));
        JobRunner jobs = new JobRunner(fixture.provider, scheduler::wakeup);
        runner.set(jobs);
        when(cycles.tick())
            .thenAnswer(
                invocation -> {
                  try {
                    runner.get().execute(observed).future().get(5, TimeUnit.SECONDS);
                  } catch (Throwable t) {
                    failure.set(t);
                  } finally {
                    retried.countDown();
                  }
                  return 60_000L;
                });
        INVOCATIONS.set(0);
        FAIL_WITH_LOADER.set(fixture.foreign);
        fixture.foreign(
            () -> {
              jobs.execute(observed).future().get(5, TimeUnit.SECONDS);
              return null;
            });
        assertTrue(retried.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(2, INVOCATIONS.get());
        assertSame(fixture.application, observed.get());
        assertSame(fixture.foreign, jobs.retrySchedulingLoader.get());
        verify(jobs.store).scheduleJobRetry(eq(jobs.job.getId()), any(), any(), eq(1), eq(1L));
      } finally {
        FAIL_WITH_LOADER.set(null);
        scheduler.stop();
      }
      fixture.assertRestored();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nullStartupLoaderFallsBackAndRepeatedStartDoesNotRecapture(boolean managed)
      throws Exception {
    try (Fixture fixture = new Fixture(managed)) {
      Thread.currentThread().setContextClassLoader(null);
      try {
        fixture.runtime.start();
      } finally {
        Thread.currentThread().setContextClassLoader(fixture.original);
      }
      fixture.foreign(
          () -> {
            fixture.runtime.start();
            assertSame(
                RatchetRuntime.class.getClassLoader(),
                fixture
                    .provider
                    .getJobExecutor()
                    .submit(() -> Thread.currentThread().getContextClassLoader())
                    .get(5, TimeUnit.SECONDS));
            return null;
          });
      fixture.assertRestored();
    }
  }

  static Stream<Arguments> operations() {
    return Stream.of(false, true)
        .flatMap(
            managed ->
                Stream.of(
                        "execute",
                        "submitRunnable",
                        "submitResult",
                        "submitCallable",
                        "scheduleRunnable",
                        "scheduleCallable",
                        "fixedRate",
                        "fixedDelay",
                        "target",
                        "invokeAll",
                        "invokeAny",
                        "failure")
                    .map(operation -> Arguments.of(managed, operation)));
  }

  @ParameterizedTest
  @MethodSource("operations")
  void allExecutorEntryPointsPinAndRestoreLoader(boolean managed, String operation)
      throws Exception {
    try (Fixture fixture = new Fixture(managed)) {
      fixture.start();
      ExecutorService jobs = fixture.provider.getJobExecutor();
      ScheduledExecutorService scheduler = fixture.provider.getScheduledExecutor();
      AtomicReference<ClassLoader> observed = new AtomicReference<>();
      CountDownLatch ran = new CountDownLatch(1);
      Runnable command =
          () -> {
            observed.set(Thread.currentThread().getContextClassLoader());
            ran.countDown();
          };
      fixture.foreign(
          () -> {
            switch (operation) {
              case "execute" -> jobs.execute(command);
              case "submitRunnable" -> jobs.submit(command).get(5, TimeUnit.SECONDS);
              case "submitResult" ->
                  assertEquals("ok", jobs.submit(command, "ok").get(5, TimeUnit.SECONDS));
              case "submitCallable" ->
                  jobs.submit(
                          () -> {
                            command.run();
                            return null;
                          })
                      .get(5, TimeUnit.SECONDS);
              case "scheduleRunnable" ->
                  scheduler.schedule(command, 0, TimeUnit.MILLISECONDS).get(5, TimeUnit.SECONDS);
              case "scheduleCallable" ->
                  scheduler
                      .schedule(
                          () -> {
                            command.run();
                            return null;
                          },
                          0,
                          TimeUnit.MILLISECONDS)
                      .get(5, TimeUnit.SECONDS);
              case "fixedRate", "fixedDelay" -> {
                CountDownLatch twice = new CountDownLatch(2);
                Runnable periodic =
                    () -> {
                      command.run();
                      twice.countDown();
                    };
                var future =
                    operation.equals("fixedRate")
                        ? scheduler.scheduleAtFixedRate(periodic, 0, 1, TimeUnit.MILLISECONDS)
                        : scheduler.scheduleWithFixedDelay(periodic, 0, 1, TimeUnit.MILLISECONDS);
                try {
                  assertTrue(twice.await(5, TimeUnit.SECONDS));
                } finally {
                  future.cancel(false);
                }
              }
              case "target" ->
                  fixture
                      .provider
                      .getJobExecutor(
                          managed || Runtime.version().feature() >= 21
                              ? ExecutorTargets.VIRTUAL
                              : ExecutorTargets.PLATFORM)
                      .orElseThrow()
                      .submit(command)
                      .get(5, TimeUnit.SECONDS);
              case "invokeAll" ->
                  jobs.invokeAll(
                          List.of(
                              () -> {
                                command.run();
                                return null;
                              }))
                      .get(0)
                      .get();
              case "invokeAny" ->
                  jobs.invokeAny(
                      List.of(
                          () -> {
                            command.run();
                            return null;
                          }));
              case "failure" ->
                  assertThrows(
                      Exception.class,
                      () ->
                          jobs.submit(
                                  () -> {
                                    command.run();
                                    Thread.currentThread().setContextClassLoader(null);
                                    throw new IllegalStateException("job failed");
                                  })
                              .get(5, TimeUnit.SECONDS));
              default -> fail(operation);
            }
            return null;
          });
      assertTrue(ran.await(5, TimeUnit.SECONDS));
      assertSame(fixture.application, observed.get(), operation);
      fixture.assertRestored();
    }
  }

  private static final class JobRunner {
    private final DefaultJobExecutorService executor;
    private final JobEntity job = new JobEntity();
    private final JobStore store = mock(JobStore.class);
    private final AtomicReference<ClassLoader> retrySchedulingLoader = new AtomicReference<>();

    JobRunner(ExecutorProvider provider) throws Exception {
      this(provider, () -> {});
    }

    JobRunner(ExecutorProvider provider, Runnable retryReady) throws Exception {
      job.setId(UUID.randomUUID());
      job.setJobType(JobExecutionType.SINGLE);
      job.setStatus(JobStatus.RUNNING);
      job.setClaimSeq(1L);
      job.setPayload(
          new JobPayload(
              ExecutorClassLoaderTest.class.getName(), "jobBody", "()V", true, List.of(), null));
      PoolRegistry pools = mock(PoolRegistry.class);
      ThreadPoolManager pool = mock(ThreadPoolManager.class);
      when(pools.pool(any())).thenReturn(pool);
      ExecutorService jobs = provider.getJobExecutor();
      when(pool.getExecutor()).thenReturn(jobs);
      when(store.getJobStatus(job.getId())).thenReturn(JobStatus.RUNNING);
      NodeIdentityProvider node = () -> "node-1";
      ExecutionObserver observer = mock(ExecutionObserver.class, RETURNS_DEEP_STUBS);
      ExecutionObserver delayed =
          new ExecutionObserver(null, null, null, null, provider, retryReady);
      doAnswer(
              invocation -> {
                retrySchedulingLoader.set(Thread.currentThread().getContextClassLoader());
                delayed.scheduleDelayedJobReadyCallback(invocation.getArgument(0));
                return null;
              })
          .when(observer)
          .scheduleDelayedJobReadyCallback(anyLong());
      when(store.incrementRetryAttempt(any(), any())).thenReturn(1);
      when(store.scheduleJobRetry(any(), any(), any(), anyInt(), any())).thenReturn(true);
      RetryPolicy retry = mock(RetryPolicy.class);
      when(retry.shouldRetry(anyInt(), any())).thenReturn(true);
      when(retry.getDelay(anyInt())).thenReturn(Duration.ofMillis(1));
      job.setMaxRetries(1);
      ResilienceStrategy resilience = mock(ResilienceStrategy.class);
      when(resilience.isServiceAvailable(anyString())).thenReturn(true);
      when(resilience.execute(anyString(), any(), any()))
          .thenAnswer(invocation -> invocation.<Callable<?>>getArgument(2).call());
      ResultPersistenceStrategy results = mock(ResultPersistenceStrategy.class);
      when(results.serialize(any(), any())).thenReturn(new SerializedJobResult(null, null));
      executor =
          new DefaultJobExecutorService(
              pools,
              mock(JobTimeoutHandler.class),
              provider,
              store,
              null,
              mock(PostExecutionHandler.class),
              node,
              observer,
              mock(PreExecutionValidator.class),
              new JobPayloadInvoker(null, name -> true),
              mock(JobSuccessFinalizer.class),
              retry,
              resilience,
              mock(ErrorSanitizer.class),
              null,
              context -> mock(JobLogger.class),
              results,
              null,
              mock(PayloadSerializer.class),
              Clock.systemUTC(),
              null);
    }

    ExecutionResult execute(AtomicReference<ClassLoader> observed) {
      job.setStatus(JobStatus.RUNNING);
      OBSERVED.set(observed);
      return executor.execute(job, "platform");
    }
  }

  private static final class Fixture implements AutoCloseable {
    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    final URLClassLoader application = new URLClassLoader(new URL[0], original);
    final URLClassLoader foreign = new URLClassLoader(new URL[0], null);
    final AtomicReference<Boolean> leaked = new AtomicReference<>();
    final ScheduledThreadPoolExecutor raw;
    final ExecutorProvider provider;
    final RatchetRuntime runtime;

    Fixture(boolean managed) throws Exception {
      raw =
          new ScheduledThreadPoolExecutor(
              2,
              runnable -> {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                thread.setContextClassLoader(foreign);
                return thread;
              }) {
            @Override
            protected void afterExecute(Runnable runnable, Throwable failure) {
              if (Thread.currentThread().getContextClassLoader() != foreign) leaked.set(true);
            }
          };
      if (managed) {
        InitialContext context = mock(InitialContext.class);
        when(context.lookup(anyString())).thenReturn(raw);
        provider =
            new DefaultExecutorProvider(
                RatchetOptions.builder()
                    .execution(e -> e.virtualExecutorJndi("java:app/virtual"))
                    .build()) {
              @Override
              InitialContext newInitialContext() {
                return context;
              }
            };
      } else {
        provider = new StandaloneExecutorProvider();
        // Create both standalone pools from the foreign thread, before runtime startup.
        foreign(
            () -> {
              provider.getJobExecutor().submit(() -> {}).get(5, TimeUnit.SECONDS);
              provider.getScheduledExecutor().submit(() -> {}).get(5, TimeUnit.SECONDS);
              return null;
            });
      }
      runtime =
          new RatchetRuntime(
              mock(Poller.class),
              mock(RecurringScheduler.class),
              mock(OrphanRecoveryTimer.class),
              mock(BatchRecoveryTimer.class),
              mock(DeadLetterService.class),
              mock(JobArchivingService.class),
              mock(LogPurgeTimer.class),
              mock(PollerWakeupListener.class),
              provider,
              mock(NodeIdentityProvider.class),
              mock(DrainController.class),
              RatchetOptions.defaults(),
              mock(JobExecutionCoordinator.class),
              mock(ClusterCoordinator.class),
              null,
              hook -> {},
              () -> {});
    }

    void start() throws Exception {
      assertThrows(
          ClassNotFoundException.class,
          () -> foreign.loadClass(ExecutorClassLoaderTest.class.getName()));
      Thread.currentThread().setContextClassLoader(application);
      try {
        runtime.start();
        Thread.currentThread().setContextClassLoader(foreign);
        runtime.start();
      } finally {
        Thread.currentThread().setContextClassLoader(original);
      }
    }

    <T> T foreign(Callable<T> action) throws Exception {
      Thread.currentThread().setContextClassLoader(foreign);
      try {
        return action.call();
      } finally {
        assertSame(
            foreign, Thread.currentThread().getContextClassLoader(), "submitting thread loader");
        Thread.currentThread().setContextClassLoader(original);
      }
    }

    void assertRestored() throws Exception {
      raw.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertNull(leaked.get(), "worker TCCL leaked");
    }

    @Override
    public void close() throws Exception {
      runtime.close();
      if (provider instanceof StandaloneExecutorProvider standalone) standalone.shutdown();
      raw.shutdownNow();
      assertTrue(raw.awaitTermination(5, TimeUnit.SECONDS));
      application.close();
      foreign.close();
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
