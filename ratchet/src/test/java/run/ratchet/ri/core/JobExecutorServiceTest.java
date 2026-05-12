package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.ExecutorProvider;

@ExtendWith(MockitoExtension.class)
class JobExecutorServiceTest {

  private static final UUID JOB_ID = new UUID(0L, 55L);
  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private ThreadPoolManager threadPoolManager;
  @Mock private JobTimeoutHandler timeoutHandler;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ExecutorService jobExecutor;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<Object> softTimeout;
  @Mock private ScheduledFuture<Object> hardTimeout;

  private JobExecutorService service;

  @BeforeEach
  void setUp() {
    service =
        new JobExecutorService(
            threadPoolManager,
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
    when(executorProvider.getJobExecutor()).thenReturn(jobExecutor);
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

  @SuppressWarnings("unchecked")
  private ExecutionResult invokeExecute(
      Callable<Void> callable, AtomicReference<JobTimeoutHandler.TimeoutHandles> handlesRef)
      throws Exception {
    Method method =
        JobExecutorService.class.getDeclaredMethod(
            "execute", UUID.class, int.class, Callable.class, AtomicReference.class);
    method.setAccessible(true);
    return (ExecutionResult) method.invoke(service, JOB_ID, 30, callable, handlesRef);
  }
}
