package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.ExecutionResult;
import run.ratchet.spi.ExecutorProvider;

@ExtendWith(MockitoExtension.class)
class JobExecutorServiceTest {

  private static final UUID JOB_ID = new UUID(0L, 55L);
  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private PoolRegistry poolRegistry;
  @Mock private ThreadPoolManager pool;
  @Mock private JobTimeoutHandler timeoutHandler;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ExecutorService jobExecutor;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<Object> softTimeout;
  @Mock private ScheduledFuture<Object> hardTimeout;

  private DefaultJobExecutorService service;

  @BeforeEach
  void setUp() {
    service =
        new DefaultJobExecutorService(
            poolRegistry,
            timeoutHandler,
            executorProvider,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            FIXED_CLOCK);
  }

  @Test
  void immediateCompletionCancelsWatchdogScheduledAfterSubmit() throws Exception {
    when(poolRegistry.pool(any())).thenReturn(pool);
    when(pool.getExecutor()).thenReturn(jobExecutor);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(jobExecutor)
        .execute(any(Runnable.class));
    when(timeoutHandler.scheduleTimeoutMonitoring(
            eq(JOB_ID), anyInt(), any(Future.class), eq(scheduledExecutor), any(Instant.class)))
        .thenReturn(new JobTimeoutHandler.TimeoutHandles(softTimeout, hardTimeout));

    AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef = new AtomicReference<>();
    ExecutionResult result = invokeExecute(() -> null, handlesRef);

    assertFalse(result.isRejected());
    assertTrue(result.future().isDone());
    verify(softTimeout).cancel(false);
    verify(hardTimeout).cancel(false);
    verify(timeoutHandler)
        .scheduleTimeoutMonitoring(
            eq(JOB_ID), anyInt(), any(Future.class), eq(scheduledExecutor), eq(FIXED_NOW));
  }

  @Test
  void shutdownRejectsLaterSubmissions() throws Exception {
    service.shutdownActiveExecutions();

    ExecutionResult result = invokeExecute(() -> null, new AtomicReference<>());

    assertTrue(result.isRejected());
  }

  @Test
  void watchdogSchedulingFailureRejectsWithoutSubmittingTask() throws Exception {
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    when(timeoutHandler.scheduleTimeoutMonitoring(
            eq(JOB_ID), anyInt(), any(Future.class), eq(scheduledExecutor), any(Instant.class)))
        .thenThrow(new RejectedExecutionException("scheduler stopped"));

    ExecutionResult result = invokeExecute(() -> null, new AtomicReference<>());

    assertTrue(result.isRejected());
    verify(pool, never()).getExecutor();
    verify(jobExecutor, never()).execute(any(Runnable.class));
  }

  @Test
  void executorLookupFailureRejectsAndClearsActiveFuture() throws Exception {
    IllegalStateException lookupFailure = new IllegalStateException("virtual executor missing");
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    when(poolRegistry.pool(any())).thenReturn(pool);
    when(pool.getExecutor()).thenThrow(lookupFailure);
    when(timeoutHandler.scheduleTimeoutMonitoring(
            eq(JOB_ID), anyInt(), any(Future.class), eq(scheduledExecutor), any(Instant.class)))
        .thenReturn(new JobTimeoutHandler.TimeoutHandles(softTimeout, hardTimeout));

    ExecutionResult result = invokeExecute(() -> null, new AtomicReference<>());

    assertTrue(result.isRejected());
    assertInstanceOf(RejectedExecutionException.class, result.exception());
    assertSame(lookupFailure, result.exception().getCause());
    assertTrue(service.awaitIdle(Duration.ZERO));
    verify(softTimeout).cancel(false);
    verify(hardTimeout).cancel(false);
    verify(jobExecutor, never()).execute(any(Runnable.class));
  }

  @Test
  void shutdownCanCancelWhileExecutorExecuteIsBlocked() throws Exception {
    CountDownLatch enteredExecute = new CountDownLatch(1);
    CountDownLatch releaseExecute = new CountDownLatch(1);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    when(poolRegistry.pool(any())).thenReturn(pool);
    when(pool.getExecutor()).thenReturn(jobExecutor);
    when(timeoutHandler.scheduleTimeoutMonitoring(
            eq(JOB_ID), anyInt(), any(Future.class), eq(scheduledExecutor), any(Instant.class)))
        .thenReturn(new JobTimeoutHandler.TimeoutHandles(softTimeout, hardTimeout));
    doAnswer(
            invocation -> {
              enteredExecute.countDown();
              assertTrue(releaseExecute.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(jobExecutor)
        .execute(any(Runnable.class));

    Thread submitter =
        new Thread(
            () -> {
              try {
                invokeExecute(() -> null, new AtomicReference<>());
              } catch (Exception e) {
                throw new AssertionError(e);
              }
            });
    submitter.start();
    assertTrue(enteredExecute.await(5, TimeUnit.SECONDS));

    assertTimeoutPreemptively(Duration.ofMillis(500), () -> service.shutdownActiveExecutions());

    releaseExecute.countDown();
    submitter.join(5_000);
    assertFalse(submitter.isAlive());
  }

  @SuppressWarnings("unchecked")
  private ExecutionResult invokeExecute(
      Callable<Void> callable, AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef)
      throws Exception {
    Method method =
        DefaultJobExecutorService.class.getDeclaredMethod(
            "execute",
            UUID.class,
            int.class,
            Callable.class,
            AtomicReference.class,
            String.class);
    method.setAccessible(true);
    return (ExecutionResult) method.invoke(service, JOB_ID, 30, callable, handlesRef, "platform");
  }
}
