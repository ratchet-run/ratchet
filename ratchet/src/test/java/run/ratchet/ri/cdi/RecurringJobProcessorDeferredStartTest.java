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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.beanFor;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.mockRecurringJobBuilder;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.recurringDefinition;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.StartupCoordinator;
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

  private final List<LogRecord> records = new ArrayList<>();
  private final Logger logger = Logger.getLogger(RecurringJobProcessor.class.getName());
  private final Handler handler =
      new Handler() {
        @Override
        public void publish(LogRecord record) {
          records.add(record);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
      };

  @BeforeEach
  void captureLogs() {
    logger.addHandler(handler);
  }

  @AfterEach
  void removeLogHandler() {
    logger.removeHandler(handler);
  }

  private void assertWarning(String text) {
    assertTrue(
        records.stream()
            .anyMatch(
                r ->
                    r.getLevel().intValue() >= Level.WARNING.intValue()
                        && r.getMessage().contains(text)),
        () ->
            "Missing warning: "
                + text
                + "; logs: "
                + records.stream().map(LogRecord::getMessage).toList());
  }

  private class CleanupFixture {
    final Deque<Runnable> retries = new ArrayDeque<>();
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    final StartupCoordinator coordinator = mock(StartupCoordinator.class);
    final RecurringAnnotationMaintenanceService maintenance =
        mock(RecurringAnnotationMaintenanceService.class);
    final Clock clock = mock(Clock.class);
    final Instant now = Instant.parse("2026-05-12T12:00:00Z");
    final BeanManager beans = mock(BeanManager.class);
    final InvocationSubmissionService submission = mock(InvocationSubmissionService.class);
    final RecurringRegistrationState state;
    final RecurringJobProcessor processor;

    CleanupFixture(boolean managed) throws Exception {
      when(clock.instant()).thenReturn(now);
      var options =
          RatchetOptions.builder().recurring(r -> r.convergenceWindowSeconds(120)).build();
      state = spy(new RecurringRegistrationState(options, clock));
      when(beans.getBeans(any(), any())).thenReturn(Set.of());
      when(coordinator.tryAcquire(any(), any())).thenReturn(true);
      processor =
          new RecurringJobProcessor(
              submission,
              mock(JobBatchStatusStore.class),
              maintenance,
              beans,
              mock(RecurringMethodInvoker.class),
              coordinator,
              state,
              options,
              Set.of(),
              clock);
      if (managed) {
        var provider = mock(ExecutorProvider.class);
        when(provider.getScheduledExecutor()).thenReturn(scheduler);
        inject(processor, "executorProvider", provider);
        when(scheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(
                i -> {
                  retries.addLast(i.getArgument(0));
                  return null;
                });
      }
    }

    void start() {
      processor.onRuntimeStart(new RatchetRuntimeStart());
    }

    void drain() {
      int count = 0;
      while (!retries.isEmpty()) {
        assertTrue(++count <= 10, "Retry budget exceeded");
        retries.removeFirst().run();
      }
    }
  }

  @Test
  void cleanupAcquisitionFailureRetriesWithOriginalPublicationAndCutoff() throws Exception {
    var f = new CleanupFixture(true);
    var failure = new IllegalStateException("transient lock");
    when(f.coordinator.tryAcquire(any(), any())).thenThrow(failure).thenReturn(true);
    f.start();
    assertEquals(f.now, f.state.registrationCompletedAt());
    when(f.clock.instant()).thenReturn(f.now.plusSeconds(600));
    // A later discovery pass must not replace the original cleanup snapshot.
    Set<Bean<?>> changedBeans = Set.of(beanFor(RecurringBean.class));
    when(f.beans.getBeans(any(), any())).thenReturn(changedBeans);
    f.drain();
    verify(f.maintenance).cancelOrphanedRecurringAnnotationJobs(Set.of(), f.now.minusSeconds(120));
    verify(f.state).markRegistrationComplete(Set.of());
    assertEquals(f.now, f.state.registrationCompletedAt());
    assertFalse(f.state.inStartupGrace());
    assertTrue(
        records.stream()
            .anyMatch(
                r ->
                    r.getThrown() == failure
                        && r.getMessage().contains("cleanup")
                        && r.getMessage().contains("1/10")));
  }

  @Test
  void cleanupCancellationFailureRetriesAndReleasesEveryLease() throws Exception {
    var f = new CleanupFixture(true);
    when(f.maintenance.cancelOrphanedRecurringAnnotationJobs(any(), any()))
        .thenThrow(new IllegalStateException("cancel failed"))
        .thenReturn(1);
    f.start();
    when(f.clock.instant()).thenReturn(f.now.plusSeconds(60));
    f.drain();
    verify(f.maintenance, times(2))
        .cancelOrphanedRecurringAnnotationJobs(Set.of(), f.now.minusSeconds(120));
    verify(f.coordinator, times(2)).release("recurring-annotation-orphan-cleanup");
    verify(f.state).markRegistrationComplete(Set.of());
  }

  @Test
  void cleanupFailuresExhaustBudgetWithTerminalWarning() throws Exception {
    var f = new CleanupFixture(true);
    when(f.maintenance.cancelOrphanedRecurringAnnotationJobs(any(), any()))
        .thenThrow(new IllegalStateException("cancel failed"));
    f.start();
    f.drain();
    verify(f.maintenance, times(10)).cancelOrphanedRecurringAnnotationJobs(any(), any());
    verify(f.coordinator, times(10)).release(any());
    verify(f.scheduler, times(9))
        .schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS));
    assertWarning("Orphan cleanup abandoned after 10");
  }

  @Test
  void cleanupUsesRemainingRegistrationBudget() throws Exception {
    var f = new CleanupFixture(true);
    Set<Bean<?>> beans = Set.of(beanFor(RecurringBean.class));
    when(f.beans.getBeans(any(), any())).thenReturn(beans);
    var builder = mockRecurringJobBuilder();
    when(f.submission.scheduleRecurringInvocation(any(), any(), any())).thenReturn(builder);
    var store = mock(RecurringJobStore.class);
    var definition = recurringDefinition(UUID.randomUUID(), "leader-gate-job");
    var lookups = new AtomicInteger();
    when(store.findRecurringByBusinessKey("leader-gate-job"))
        .thenAnswer(
            i -> lookups.incrementAndGet() < 9 ? Optional.empty() : Optional.of(definition));
    injectResolvableRecurringStore(f.processor, store);
    when(f.coordinator.tryAcquire(any(), any()))
        .thenThrow(new IllegalStateException("lock failed"));
    f.start();
    f.drain();
    verify(builder, times(9)).submit();
    verify(f.coordinator, times(2)).tryAcquire(any(), any());
    verify(f.coordinator, never()).release(any());
    verify(f.scheduler, times(9))
        .schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS));
    verify(f.state).markRegistrationComplete(Set.of("leader-gate-job"));
    assertWarning("Orphan cleanup abandoned after 10");
  }

  @Test
  void leaseContentionCompletesCleanupWithoutRetryOrRelease() throws Exception {
    var f = new CleanupFixture(true);
    when(f.coordinator.tryAcquire(any(), any())).thenReturn(false);
    f.start();
    f.start();
    verify(f.coordinator).tryAcquire(any(), any());
    verify(f.coordinator, never()).release(any());
    verifyNoInteractions(f.maintenance, f.scheduler);
  }

  @Test
  void releaseFailureDoesNotRepeatSuccessfulCancellation() throws Exception {
    var f = new CleanupFixture(true);
    doThrow(new IllegalStateException("release failed")).when(f.coordinator).release(any());
    f.start();
    f.start();
    verify(f.maintenance).cancelOrphanedRecurringAnnotationJobs(any(), any());
    verifyNoInteractions(f.scheduler);
  }

  @Test
  void inlineAcquisitionFailureIsNonfatalAndWarns() throws Exception {
    var f = new CleanupFixture(false);
    when(f.coordinator.tryAcquire(any(), any()))
        .thenThrow(new IllegalStateException("lock failed"));
    assertDoesNotThrow(f::start);
    assertWarning("Orphan cleanup abandoned");
    verify(f.coordinator).tryAcquire(any(), any());
  }

  @Test
  void inlineCancellationFailureIsNonfatalAndWarns() throws Exception {
    var f = new CleanupFixture(false);
    when(f.maintenance.cancelOrphanedRecurringAnnotationJobs(any(), any()))
        .thenThrow(new IllegalStateException("cancel failed"));
    assertDoesNotThrow(() -> f.processor.onStartup(new Object()));
    assertWarning("Orphan cleanup abandoned");
    verify(f.coordinator).release(any());
  }

  @Test
  void rejectedCleanupRetryWarnsAboutUnfinishedCleanup() throws Exception {
    var f = new CleanupFixture(true);
    when(f.coordinator.tryAcquire(any(), any()))
        .thenThrow(new IllegalStateException("lock failed"));
    when(f.scheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new RejectedExecutionException("shutdown"));
    assertDoesNotThrow(f::start);
    assertWarning("Orphan cleanup abandoned");
    assertWarning("scheduling rejected");
  }

  @Test
  void rejectedInitialSchedulingWarnsAboutUnfinishedRegistration() throws Exception {
    var f = new CleanupFixture(true);
    when(f.scheduler.schedule(any(Runnable.class), eq(500L), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new RejectedExecutionException("shutdown"));
    assertDoesNotThrow(() -> f.processor.onStartup(new Object()));
    assertWarning("registration and orphan cleanup");
  }

  @Test
  void overlappingFinalizationCancelsOnlyOnce() throws Exception {
    var f = new CleanupFixture(true);
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    when(f.maintenance.cancelOrphanedRecurringAnnotationJobs(any(), any()))
        .thenAnswer(
            i -> {
              entered.countDown();
              assertTrue(release.await(5, TimeUnit.SECONDS));
              return 0;
            });
    var threads = Executors.newFixedThreadPool(2);
    try {
      var first = threads.submit(f::start);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      var secondStarted = new CountDownLatch(1);
      var second =
          threads.submit(
              () -> {
                secondStarted.countDown();
                f.start();
              });
      assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
      release.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      f.start();
      verify(f.maintenance).cancelOrphanedRecurringAnnotationJobs(any(), any());
      verify(f.state).markRegistrationComplete(Set.of());
    } finally {
      release.countDown();
      threads.shutdownNow();
    }
  }

  @Test
  void failedRegistrationCannotPublishEvenWhenOldMasterExists() throws Exception {
    var f = new CleanupFixture(false);
    Set<Bean<?>> recurringBeans = Set.of(beanFor(RecurringBean.class));
    when(f.beans.getBeans(any(), any())).thenReturn(recurringBeans);
    var failure = new IllegalArgumentException("invalid invocation descriptor");
    when(f.submission.scheduleRecurringInvocation(any(), any(), any())).thenThrow(failure);
    var store = mock(RecurringJobStore.class);
    when(store.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(Optional.of(recurringDefinition(UUID.randomUUID(), "leader-gate-job")));
    injectResolvableRecurringStore(f.processor, store);

    var thrown = assertThrows(IllegalStateException.class, f::start);
    assertEquals(failure, thrown.getCause());
    verify(f.state, never()).markRegistrationComplete(any());
    verifyNoInteractions(f.maintenance);
  }

  @Test
  void failedRegistrationWithoutStoreRetriesAndReportsAbandonment() throws Exception {
    var f = new CleanupFixture(true);
    Set<Bean<?>> recurringBeans = Set.of(beanFor(RecurringBean.class));
    when(f.beans.getBeans(any(), any())).thenReturn(recurringBeans);
    when(f.submission.scheduleRecurringInvocation(any(), any(), any()))
        .thenThrow(new IllegalArgumentException("invalid invocation descriptor"));

    f.start();
    f.drain();

    verify(f.submission, times(10)).scheduleRecurringInvocation(any(), any(), any());
    verify(f.state, never()).markRegistrationComplete(any());
    verifyNoInteractions(f.maintenance);
    assertWarning("@Recurring registration and orphan cleanup abandoned");
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
    var instance = mock(Instance.class);
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
