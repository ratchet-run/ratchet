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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.beanFor;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.mockRecurringJobBuilder;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.recurringDefinition;

import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.Recurring;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Verifies the onStartup()/onRuntimeStart() split added to defer registration on build-time-CDI
 * runtimes (e.g. Quarkus) until {@link RatchetRuntimeStart} fires. This is the highest-risk code in
 * the ratchet-quarkus branch: a regression here would silently stop @Recurring registration on
 * every runtime, or double-register it, with no automated signal before this test existed.
 */
class RecurringJobProcessorDeferredStartTest {

  @AfterEach
  void clearDeferFlag() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
  }

  @Test
  void onStartup_whenAutoStartDeferred_doesNotRegisterJobs() {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var processor = newProcessor(invocationSubmissionService);

    processor.onStartup(new Object());

    verifyNoInteractions(invocationSubmissionService);
  }

  @Test
  void onStartup_whenNotDeferred_andNoManagedExecutor_registersInline() throws Exception {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var processor = newProcessor(invocationSubmissionService);

    processor.onStartup(new Object());

    // No ExecutorProvider is injected via this constructor, matching the documented plain-CDI/SE/
    // unit-test path: registration happens inline on the calling thread, not deferred.
    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  @Test
  void onRuntimeStart_registersJobs_evenWhileAutoStartIsDeferred() throws Exception {
    // The realistic Quarkus scenario: the defer flag stays true for the whole process lifetime,
    // and RatchetRuntimeStart is the only thing that ever triggers registration.
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var processor = newProcessor(invocationSubmissionService);

    processor.onRuntimeStart(new RatchetRuntimeStart());

    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  @Test
  void onRuntimeStart_whenRegistrationIsNotCommitted_retriesOnManagedScheduler() throws Exception {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var processor = newProcessor(invocationSubmissionService);

    var managedScheduler = mock(ScheduledExecutorService.class);
    var executorProvider = mock(ExecutorProvider.class);
    when(executorProvider.getScheduledExecutor()).thenReturn(managedScheduler);
    inject(processor, "executorProvider", executorProvider);

    var recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(
            Optional.empty(),
            Optional.of(recurringDefinition(UUID.randomUUID(), "leader-gate-job")));
    injectResolvableRecurringStore(processor, recurringJobStore);

    processor.onRuntimeStart(new RatchetRuntimeStart());

    ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
    verify(managedScheduler).schedule(retry.capture(), eq(500L), eq(TimeUnit.MILLISECONDS));

    retry.getValue().run();

    verify(invocationSubmissionService, times(2))
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder, times(2)).submit();
    verify(recurringJobStore, times(2)).findRecurringByBusinessKey("leader-gate-job");
  }

  @Test
  void onRuntimeStart_whenRegistrationNeverCommits_stopsAfterBoundedAttempts() throws Exception {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var processor = newProcessor(invocationSubmissionService);

    Deque<Runnable> retries = new ArrayDeque<>();
    var managedScheduler = mock(ScheduledExecutorService.class);
    when(managedScheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(
            invocation -> {
              retries.addLast(invocation.getArgument(0));
              return null;
            });
    var executorProvider = mock(ExecutorProvider.class);
    when(executorProvider.getScheduledExecutor()).thenReturn(managedScheduler);
    inject(processor, "executorProvider", executorProvider);

    var recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(Optional.empty());
    injectResolvableRecurringStore(processor, recurringJobStore);

    processor.onRuntimeStart(new RatchetRuntimeStart());
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

  private RecurringJobProcessor newProcessor(
      InvocationSubmissionService invocationSubmissionService) {
    var beanManager = mock(BeanManager.class);
    Set<Bean<?>> beans = Set.of(beanFor(RecurringBean.class));
    when(beanManager.getBeans(any(), any())).thenReturn(beans);
    return new RecurringJobProcessor(
        invocationSubmissionService,
        mock(JobBatchStatusStore.class),
        mock(RecurringAnnotationMaintenanceService.class),
        beanManager,
        mock(RecurringMethodInvoker.class),
        null,
        new RecurringRegistrationState());
  }

  @SuppressWarnings("unchecked")
  private static void injectResolvableRecurringStore(
      RecurringJobProcessor processor, RecurringJobStore recurringJobStore) throws Exception {
    var instance = mock(jakarta.enterprise.inject.Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(recurringJobStore);
    inject(processor, "recurringJobStoreInstance", instance);
  }

  private static void inject(RecurringJobProcessor processor, String fieldName, Object value)
      throws Exception {
    Field field = RecurringJobProcessor.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(processor, value);
  }

  static class RecurringBean {
    @Recurring(id = "leader-gate-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }
}
