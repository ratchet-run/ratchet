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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
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
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RecurringJobExecutor;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;

class DefaultRecurringSchedulerTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void calculateNextDelay_usesInjectedClock() throws Exception {
    RecurringJobStore jobCrudStore = mock(RecurringJobStore.class);
    when(jobCrudStore.findEarliestRecurringNextFire()).thenReturn(Optional.of(NOW.plusSeconds(10)));

    DefaultRecurringScheduler scheduler = scheduler(jobCrudStore);

    assertEquals(9500L, calculateNextDelay(scheduler, 0));
  }

  @Test
  void calculateNextDelay_noRecurringJobsUsesMaxPoll() throws Exception {
    RecurringJobStore jobCrudStore = mock(RecurringJobStore.class);
    when(jobCrudStore.findEarliestRecurringNextFire()).thenReturn(Optional.empty());

    DefaultRecurringScheduler scheduler = scheduler(jobCrudStore);

    assertEquals(60000L, calculateNextDelay(scheduler, 0));
  }

  @Test
  void calculateNextDelay_processedJobsUsesMinPoll() throws Exception {
    DefaultRecurringScheduler scheduler = scheduler(mock(RecurringJobStore.class));

    assertEquals(1000L, calculateNextDelay(scheduler, 1));
  }

  @Test
  void effectiveClockFailsFastWhenProxyConstructorInstanceIsUsed() throws Exception {
    DefaultRecurringScheduler scheduler = new DefaultRecurringScheduler();
    Method method = DefaultRecurringScheduler.class.getDeclaredMethod("effective");
    method.setAccessible(true);

    InvocationTargetException thrown =
        assertThrows(InvocationTargetException.class, () -> method.invoke(scheduler));

    assertInstanceOf(IllegalStateException.class, thrown.getCause());
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
    when(executor.scheduleWithFixedDelay(any(Runnable.class), eq(2L), eq(2L), eq(TimeUnit.MINUTES)))
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

    DefaultRecurringScheduler scheduler =
        new DefaultRecurringScheduler(
            executorProvider,
            mock(run.ratchet.store.spi.RecurringJobStore.class),
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

  private static DefaultRecurringScheduler scheduler(
      run.ratchet.store.spi.RecurringJobStore recurringJobStore) {
    DefaultRecurringScheduler scheduler =
        new DefaultRecurringScheduler(
            mock(ExecutorProvider.class),
            recurringJobStore,
            mock(SingletonLeaseService.class),
            mock(NodeIdentityProvider.class),
            mock(RecurringJobExecutor.class),
            mock(PollerScheduler.class),
            Clock.fixed(NOW, ZoneOffset.UTC));
    scheduler.configure(1000L, 60000L, 20);
    return scheduler;
  }

  private static long calculateNextDelay(DefaultRecurringScheduler scheduler, int processedCount)
      throws Exception {
    Method method =
        DefaultRecurringScheduler.class.getDeclaredMethod("calculateNextDelay", int.class);
    method.setAccessible(true);
    return (long) method.invoke(scheduler, processedCount);
  }
}
