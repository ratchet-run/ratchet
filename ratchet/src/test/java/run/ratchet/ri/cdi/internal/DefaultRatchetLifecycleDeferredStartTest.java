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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.RatchetRuntimeStart;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * Verifies the onStartup()/onRuntimeStart() split added to defer engine start on build-time-CDI
 * runtimes (e.g. Quarkus) until {@link RatchetRuntimeStart} fires. Mirrors
 * RecurringJobProcessorDeferredStartTest for the sibling observer that starts the poller and
 * recurring scheduler.
 */
class DefaultRatchetLifecycleDeferredStartTest {

  @AfterEach
  void clearDeferFlag() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
  }

  @Test
  void onStartup_whenAutoStartDeferred_doesNotStart() {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    Poller poller = mock(Poller.class);
    RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    DefaultRatchetLifecycle lifecycle = newLifecycle(poller, recurringScheduler);

    lifecycle.onStartup(new Object());

    verifyNoInteractions(poller);
    verifyNoInteractions(recurringScheduler);
  }

  @Test
  void onStartup_whenNotDeferred_startsImmediately() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
    Poller poller = mock(Poller.class);
    RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    DefaultRatchetLifecycle lifecycle = newLifecycle(poller, recurringScheduler);

    lifecycle.onStartup(new Object());

    verify(poller).init();
  }

  @Test
  void onRuntimeStart_starts_evenWhileAutoStartIsDeferred() {
    // The realistic Quarkus scenario: the defer flag stays true for the whole process lifetime,
    // and RatchetRuntimeStart is the only thing that ever triggers start().
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    Poller poller = mock(Poller.class);
    RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    DefaultRatchetLifecycle lifecycle = newLifecycle(poller, recurringScheduler);

    lifecycle.onRuntimeStart(new RatchetRuntimeStart());

    verify(poller).init();
  }

  private DefaultRatchetLifecycle newLifecycle(
      Poller poller, RecurringScheduler recurringScheduler) {
    return new DefaultRatchetLifecycle(
        poller,
        recurringScheduler,
        mock(OrphanRecoveryTimer.class),
        mock(BatchRecoveryTimer.class),
        mock(DeadLetterService.class),
        mock(JobArchivingService.class),
        mock(LogPurgeTimer.class),
        mock(PollerWakeupListener.class),
        executorProviderWithScheduler(),
        mock(NodeIdentityProvider.class),
        mock(DrainController.class),
        quietOptions(),
        mock(JobExecutionCoordinator.class),
        mock(ClusterCoordinator.class));
  }

  private ExecutorProvider executorProviderWithScheduler() {
    ExecutorProvider provider = mock(ExecutorProvider.class);
    when(provider.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    return provider;
  }

  private RatchetOptions quietOptions() {
    return RatchetOptions.builder()
        .node(node -> node.orphanScanIntervalMinutes(1L))
        .maintenance(
            maintenance ->
                maintenance.dlqPurgeEnabled(false).jobArchiveEnabled(false).logPurgeEnabled(false))
        .build();
  }
}
