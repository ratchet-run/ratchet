package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.LockStore;

class RecurringSchedulerTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void calculateNextDelay_usesInjectedClock() throws Exception {
    JobCrudStore jobCrudStore = mock(JobCrudStore.class);
    when(jobCrudStore.findEarliestRecurringNextFire()).thenReturn(Optional.of(NOW.plusSeconds(10)));

    RecurringScheduler scheduler = scheduler(jobCrudStore);

    assertEquals(9500L, calculateNextDelay(scheduler, 0));
  }

  @Test
  void calculateNextDelay_noRecurringJobsUsesMaxPoll() throws Exception {
    JobCrudStore jobCrudStore = mock(JobCrudStore.class);
    when(jobCrudStore.findEarliestRecurringNextFire()).thenReturn(Optional.empty());

    RecurringScheduler scheduler = scheduler(jobCrudStore);

    assertEquals(60000L, calculateNextDelay(scheduler, 0));
  }

  @Test
  void calculateNextDelay_processedJobsUsesMinPoll() throws Exception {
    RecurringScheduler scheduler = scheduler(mock(JobCrudStore.class));

    assertEquals(1000L, calculateNextDelay(scheduler, 1));
  }

  @Test
  void run_failedLeaseRenewalReleasesLeaseAndStopsScheduling() {
    var executorProvider = mock(ExecutorProvider.class);
    var executor = mock(ScheduledExecutorService.class);
    var scheduledScan = mock(ScheduledFuture.class);
    var renewalTask = mock(ScheduledFuture.class);
    when(executorProvider.getScheduledExecutor()).thenReturn(executor);
    when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(scheduledScan);
    AtomicReference<Runnable> renewal = new AtomicReference<>();
    when(executor.scheduleAtFixedRate(any(Runnable.class), eq(2L), eq(2L), eq(TimeUnit.MINUTES)))
        .thenAnswer(
            invocation -> {
              renewal.set(invocation.getArgument(0));
              return renewalTask;
            });

    var lockStore = mock(LockStore.class);
    var lease = new SingletonLease(lockStore, "recurringScheduler", "node-1");
    when(lockStore.renewLock("recurringScheduler", Duration.ofMinutes(5), "node-1"))
        .thenReturn(false);
    var singletonLeaseService = mock(SingletonLeaseService.class);
    when(singletonLeaseService.tryAcquire("recurringScheduler", Duration.ofMinutes(5)))
        .thenReturn(Optional.of(lease));
    var nodeIdentityProvider = mock(NodeIdentityProvider.class);
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    var recurringJobExecutor = mock(RecurringJobExecutor.class);
    when(recurringJobExecutor.process(20, "node-1"))
        .thenAnswer(
            invocation -> {
              renewal.get().run();
              return 1;
            });
    var pollerScheduler = mock(PollerScheduler.class);

    RecurringScheduler scheduler =
        new RecurringScheduler(
            executorProvider,
            mock(JobCrudStore.class),
            singletonLeaseService,
            nodeIdentityProvider,
            recurringJobExecutor,
            pollerScheduler,
            Clock.fixed(NOW, ZoneOffset.UTC));
    scheduler.init();
    clearInvocations(executor);

    scheduler.run();

    verify(lockStore).unlock("recurringScheduler", "node-1");
    verify(scheduledScan).cancel(false);
    verify(renewalTask).cancel(false);
    verify(pollerScheduler, never()).wakeup();
    verify(executor, never()).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  private static RecurringScheduler scheduler(JobCrudStore jobCrudStore) {
    RecurringScheduler scheduler =
        new RecurringScheduler(
            mock(ExecutorProvider.class),
            jobCrudStore,
            mock(SingletonLeaseService.class),
            mock(NodeIdentityProvider.class),
            mock(RecurringJobExecutor.class),
            mock(PollerScheduler.class),
            Clock.fixed(NOW, ZoneOffset.UTC));
    scheduler.configure(1000L, 60000L, 20);
    return scheduler;
  }

  private static long calculateNextDelay(RecurringScheduler scheduler, int processedCount)
      throws Exception {
    Method method = RecurringScheduler.class.getDeclaredMethod("calculateNextDelay", int.class);
    method.setAccessible(true);
    return (long) method.invoke(scheduler, processedCount);
  }
}
