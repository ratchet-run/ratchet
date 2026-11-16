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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.mockito.MockSettings;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.internal.*;
import run.ratchet.spi.*;
import run.ratchet.store.spi.*;

class DurableStartupGateTest {
  @Test
  void missingOutboxFailsBeforeHooksOrWorkers() {
    gate(false, true, true, true, "EventOutboxStore");
  }

  @Test
  void missingLockFailsBeforeHooksOrWorkers() {
    gate(true, false, true, true, "LockStore");
  }

  @Test
  void missingExecutorFailsBeforeHooksOrWorkers() {
    gate(true, true, false, true, "scheduled executor");
  }

  @Test
  void disabledModeDoesNotCheckCapabilities() {
    gate(false, false, true, false, null);
  }

  @Test
  void injectedDeleterWithoutOutboxFailsValidation() {
    assertMissingBeanType(validation(true, false, true, true), "EventOutboxStore");
  }

  @Test
  void injectedRelayWithoutOutboxFailsValidation() {
    assertMissingBeanType(validation(true, true, false, true), "EventOutboxStore");
  }

  @Test
  void leaseServiceWithoutLockStoreFailsValidation() {
    assertMissingBeanType(validation(true, true, true, false), "LockStore");
  }

  @Test
  void injectedCapabilitiesPassValidation() {
    assertDoesNotThrow(validation(true, true, true, true)::validate);
  }

  @Test
  void disabledModeIgnoresMissingInjectedCapabilities() {
    assertDoesNotThrow(validation(false, false, false, false)::validate);
  }

  @Test
  void nonCdiWorkersCanBeCreatedAtStartup() {
    JobStore store = capableStore();
    DurableTerminalEvents durable =
        new DurableTerminalEvents(
            RatchetOptions.builder().events(e -> e.durable(true)).build(),
            store,
            Clock.systemUTC(),
            new InternalEventPublisher(),
            mock(AfterCommitRegistrar.class),
            new SingletonLeaseService((LockStore) store, mock(NodeIdentityProvider.class)));
    assertDoesNotThrow(durable::validate);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    try {
      assertDoesNotThrow(() -> durable.start(executor));
      assertTrue(durable.relay().hasStore());
    } finally {
      durable.stop();
    }
  }

  private JobStore capableStore() {
    return mock(
        JobStore.class,
        withSettings()
            .extraInterfaces(EventOutboxStore.class, LockStore.class)
            .defaultAnswer(CALLS_REAL_METHODS));
  }

  private DurableTerminalEvents validation(
      boolean enabled, boolean deleterStore, boolean relayStore, boolean lockStore) {
    JobStore store = capableStore();
    RatchetOptions options = RatchetOptions.builder().events(e -> e.durable(enabled)).build();
    SingletonLeaseService leases =
        new SingletonLeaseService(
            lockStore ? (LockStore) store : null, mock(NodeIdentityProvider.class));
    InternalEventPublisher publisher = new InternalEventPublisher();
    return new DurableTerminalEvents(
        options,
        store,
        Clock.systemUTC(),
        publisher,
        mock(AfterCommitRegistrar.class),
        leases,
        new OutboxDeleter(deleterStore ? (EventOutboxStore) store : null),
        new TerminalEventRelay(
            relayStore ? (EventOutboxStore) store : null,
            null,
            leases,
            publisher,
            options,
            Clock.systemUTC()));
  }

  private void assertMissingBeanType(DurableTerminalEvents durable, String capability) {
    String message = assertThrows(IllegalStateException.class, durable::validate).getMessage();
    assertTrue(message.contains(capability));
    assertTrue(message.contains("implements"));
    assertTrue(message.contains("does not expose it as a bean type"));
    assertTrue(message.contains("list every capability"));
  }

  private void gate(
      boolean outbox, boolean lock, boolean executorAvailable, boolean enabled, String message) {
    MockSettings settings = withSettings();
    if (outbox && lock) settings.extraInterfaces(EventOutboxStore.class, LockStore.class);
    else if (outbox) settings.extraInterfaces(EventOutboxStore.class);
    else if (lock) settings.extraInterfaces(LockStore.class);
    JobStore store = mock(JobStore.class, settings.defaultAnswer(CALLS_REAL_METHODS));
    RatchetOptions options = RatchetOptions.builder().events(e -> e.durable(enabled)).build();
    DurableTerminalEvents durable =
        new DurableTerminalEvents(
            options,
            store,
            Clock.systemUTC(),
            new InternalEventPublisher(),
            mock(AfterCommitRegistrar.class),
            new SingletonLeaseService(
                lock ? (LockStore) store : null, mock(NodeIdentityProvider.class)));
    DeadLetterService deadLetter = mock(DeadLetterService.class);
    ExecutorProvider executor = mock(ExecutorProvider.class);
    if (executorAvailable)
      when(executor.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    Runnable workers = mock(Runnable.class);
    SchedulerLifecycleHook hook = mock(SchedulerLifecycleHook.class);
    Poller poller = mock(Poller.class);
    RatchetRuntime runtime =
        new RatchetRuntime(
            poller,
            mock(RecurringScheduler.class),
            mock(OrphanRecoveryTimer.class),
            mock(BatchRecoveryTimer.class),
            mock(CancelRequestRelay.class),
            mock(JobStateFlusher.class),
            deadLetter,
            durable,
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executor,
            mock(NodeIdentityProvider.class),
            mock(DrainController.class),
            options,
            mock(JobExecutionCoordinator.class),
            mock(ClusterCoordinator.class),
            () -> List.of(hook),
            ignored -> {},
            workers);
    if (message == null) {
      runtime.start();
      verify(store, never()).capability(EventOutboxStore.class);
      verify(store, never()).capability(LockStore.class);
      runtime.close();
    } else {
      assertTrue(
          assertThrows(IllegalStateException.class, runtime::start).getMessage().contains(message));
      verifyNoInteractions(workers, hook, poller);
    }
  }
}
