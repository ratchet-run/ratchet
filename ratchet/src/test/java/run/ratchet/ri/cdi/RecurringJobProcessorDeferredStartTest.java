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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.mockRecurringJobBuilder;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.recurringDefinition;

import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringMethodRegistrar;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Verifies the CDI startup adapters and the portable registrar's deferred retry lifecycle.
 *
 * <p>A regression here would silently stop recurring registration on build-time CDI runtimes, run
 * registration twice, or allow a retry to outlive its runtime.
 */
class RecurringJobProcessorDeferredStartTest {

  private static final RecurringMethodDiscovery DISCOVERY = () -> Set.of(RecurringBean.class);

  @AfterEach
  void clearDeferFlag() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
  }

  @Test
  void onStartup_whenAutoStartDeferred_doesNotRegisterJobs() {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    RecurringJobProcessor processor =
        new RecurringJobProcessor(newRegistrar(invocationSubmissionService, null, null));

    processor.onStartup(new Object());

    verifyNoInteractions(invocationSubmissionService);
  }

  @Test
  void onStartup_whenNotDeferredAndNoManagedExecutor_registersInline() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    RecurringJobProcessor processor =
        new RecurringJobProcessor(newRegistrar(invocationSubmissionService, null, null));

    processor.onStartup(new Object());

    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  @Test
  void onRuntimeStart_registersJobsEvenWhileAutoStartIsDeferred() {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    RecurringJobProcessor processor =
        new RecurringJobProcessor(newRegistrar(invocationSubmissionService, null, null));

    processor.onRuntimeStart(new RatchetRuntimeStart());

    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  @Test
  void runtimeReadyRegistration_whenNotCommitted_retriesOnManagedScheduler() {
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    ScheduledExecutorService managedScheduler = mock(ScheduledExecutorService.class);
    ExecutorProvider executorProvider = executorProvider(managedScheduler);
    RecurringJobStore recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(
            Optional.empty(),
            Optional.of(recurringDefinition(UUID.randomUUID(), "leader-gate-job")));
    RecurringMethodRegistrar registrar =
        newRegistrar(
            invocationSubmissionService, executorProvider, jobStoreAdvertising(recurringJobStore));

    registrar.register();

    ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
    verify(managedScheduler).schedule(retry.capture(), eq(500L), eq(TimeUnit.MILLISECONDS));
    retry.getValue().run();

    verify(invocationSubmissionService, times(2))
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder, times(2)).submit();
    verify(recurringJobStore, times(2)).findRecurringByBusinessKey("leader-gate-job");
  }

  @Test
  void runtimeReadyRegistration_whenNeverCommitted_stopsAfterBoundedAttempts() {
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    Deque<Runnable> retries = new ArrayDeque<>();
    ScheduledExecutorService managedScheduler = mock(ScheduledExecutorService.class);
    when(managedScheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(
            invocation -> {
              retries.addLast(invocation.getArgument(0));
              return null;
            });
    RecurringJobStore recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(Optional.empty());
    RecurringMethodRegistrar registrar =
        newRegistrar(
            invocationSubmissionService,
            executorProvider(managedScheduler),
            jobStoreAdvertising(recurringJobStore));

    registrar.register();
    while (!retries.isEmpty()) {
      retries.removeFirst().run();
    }

    verify(invocationSubmissionService, times(10))
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder, times(10)).submit();
    verify(recurringJobStore, times(10)).findRecurringByBusinessKey("leader-gate-job");
    verify(managedScheduler, times(9))
        .schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  void pendingDeferredRetryBecomesNoOpAfterCancellation() {
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    ScheduledExecutorService managedScheduler = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
    when(managedScheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(invocation -> scheduledFuture);
    RecurringJobStore recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(Optional.empty());
    RecurringMethodRegistrar registrar =
        newRegistrar(
            invocationSubmissionService,
            executorProvider(managedScheduler),
            jobStoreAdvertising(recurringJobStore));

    registrar.register();

    ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
    verify(managedScheduler).schedule(retry.capture(), eq(500L), eq(TimeUnit.MILLISECONDS));
    registrar.cancel();
    verify(scheduledFuture).cancel(false);
    retry.getValue().run();

    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
    verify(recurringJobStore).findRecurringByBusinessKey("leader-gate-job");
  }

  @Test
  void cancellationWhileSchedulingCancelsTheLateFuture() {
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    ScheduledExecutorService managedScheduler = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
    AtomicReference<RecurringMethodRegistrar> registrarReference = new AtomicReference<>();
    when(managedScheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(
            invocation -> {
              registrarReference.get().cancel();
              return scheduledFuture;
            });
    RecurringMethodRegistrar registrar =
        newRegistrar(invocationSubmissionService, executorProvider(managedScheduler), null);
    registrarReference.set(registrar);

    registrar.registerFromApplicationStart();

    verify(scheduledFuture).cancel(false);
    verifyNoInteractions(invocationSubmissionService);
  }

  @Test
  void cancellationIsIdempotent() {
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    ScheduledExecutorService managedScheduler = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
    when(managedScheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(invocation -> scheduledFuture);
    RecurringMethodRegistrar registrar =
        newRegistrar(invocationSubmissionService, executorProvider(managedScheduler), null);

    assertDoesNotThrow(
        () -> {
          registrar.registerFromApplicationStart();
          registrar.cancel();
          registrar.cancel();
        });

    verify(scheduledFuture).cancel(false);
    verifyNoInteractions(invocationSubmissionService);
  }

  @Test
  void sameRegistrarRestartUsesNewGenerationAndLeavesStaleRetryInert() {
    InvocationSubmissionService invocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    ScheduledExecutorService managedScheduler = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> staleFuture = mock(ScheduledFuture.class);
    when(managedScheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(invocation -> staleFuture);
    RecurringJobStore recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(Optional.of(recurringDefinition(UUID.randomUUID(), "leader-gate-job")));
    RecurringMethodRegistrar registrar =
        newRegistrar(
            invocationSubmissionService,
            executorProvider(managedScheduler),
            jobStoreAdvertising(recurringJobStore));

    registrar.registerFromApplicationStart();

    ArgumentCaptor<Runnable> staleRetry = ArgumentCaptor.forClass(Runnable.class);
    verify(managedScheduler).schedule(staleRetry.capture(), eq(500L), eq(TimeUnit.MILLISECONDS));
    registrar.cancel();
    registrar.register();
    staleRetry.getValue().run();

    verify(staleFuture).cancel(false);
    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
    verify(recurringJobStore).findRecurringByBusinessKey("leader-gate-job");
  }

  @Test
  void freshRegistrarCanRegisterAfterPreviousRegistrarWasCancelled() {
    InvocationSubmissionService oldInvocationSubmissionService =
        mock(InvocationSubmissionService.class);
    RecurringMethodRegistrar oldRegistrar =
        newRegistrar(oldInvocationSubmissionService, null, null);
    oldRegistrar.cancel();

    InvocationSubmissionService freshInvocationSubmissionService =
        mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(freshInvocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    RecurringMethodRegistrar freshRegistrar =
        newRegistrar(freshInvocationSubmissionService, null, null);

    freshRegistrar.register();

    verifyNoInteractions(oldInvocationSubmissionService);
    verify(freshInvocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  private static RecurringMethodRegistrar newRegistrar(
      InvocationSubmissionService invocationSubmissionService,
      ExecutorProvider executorProvider,
      JobStore jobStore) {
    return new RecurringMethodRegistrar(
        invocationSubmissionService,
        mock(RecurringAnnotationMaintenanceService.class),
        DISCOVERY,
        mock(RecurringMethodInvoker.class),
        null,
        new RecurringRegistrationState(),
        RatchetOptions.defaults(),
        executorProvider,
        jobStore,
        Clock.systemUTC());
  }

  private static ExecutorProvider executorProvider(ScheduledExecutorService scheduler) {
    ExecutorProvider executorProvider = mock(ExecutorProvider.class);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduler);
    return executorProvider;
  }

  private static JobStore jobStoreAdvertising(RecurringJobStore recurringJobStore) {
    JobStore jobStore = mock(JobStore.class);
    when(jobStore.capability(RecurringJobStore.class)).thenReturn(Optional.of(recurringJobStore));
    return jobStore;
  }

  static class RecurringBean {
    @Recurring(id = "leader-gate-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }
}
