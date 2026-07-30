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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.DefaultRatchetRuntime;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.RecurringRegistration;
import run.ratchet.ri.core.internal.RuntimeInstallation;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;

class RatchetRuntimeLifecycleContractTest {

  @Test
  void schemaFailureBeforeStartUninstallsEverySeamWithoutTouchingNodeTimersOrWorkers() {
    RuntimeFixture fixture = new RuntimeFixture();
    RecordingInstallation firstInstallation = new RecordingInstallation();
    RecordingInstallation secondInstallation = new RecordingInstallation();
    SchedulerLifecycleHook failingHook = mock(SchedulerLifecycleHook.class);
    SchemaInitializationException failure =
        new SchemaInitializationException("schema is not initialized");
    doThrow(failure).when(failingHook).beforeStart();

    DefaultRatchetRuntime runtime =
        fixture.runtimeWithScheduledWork(
            List.of(failingHook), List.of(firstInstallation, secondInstallation));

    assertSame(failure, assertThrows(SchemaInitializationException.class, runtime::start));
    assertNull(firstInstallation.owner());
    assertNull(secondInstallation.owner());
    assertEquals(1, firstInstallation.installCalls());
    assertEquals(1, firstInstallation.uninstallCalls());
    assertEquals(1, secondInstallation.installCalls());
    assertEquals(1, secondInstallation.uninstallCalls());
    verify(fixture.recurringRegistration, never()).register();
    verify(fixture.recurringRegistration).cancel();
    assertRuntimeServicesUntouched(fixture);
  }

  @Test
  void sharedInstallationConflictFailsSecondRuntimeWithoutDisplacingFirst() {
    RecordingInstallation installation = new RecordingInstallation();
    RuntimeFixture firstFixture = new RuntimeFixture();
    RuntimeFixture secondFixture = new RuntimeFixture();
    SchedulerLifecycleHook secondHook = mock(SchedulerLifecycleHook.class);
    DefaultRatchetRuntime firstRuntime = firstFixture.runtime(List.of(), List.of(installation));
    DefaultRatchetRuntime secondRuntime =
        secondFixture.runtime(List.of(secondHook), List.of(installation));

    firstRuntime.start();

    assertThrows(IllegalStateException.class, secondRuntime::start);
    assertSame(firstRuntime, installation.owner());
    verifyNoInteractions(secondHook);
    verify(secondFixture.recurringRegistration, never()).register();
    verify(secondFixture.recurringRegistration).cancel();
    assertRuntimeServicesUntouched(secondFixture);

    firstRuntime.stop();
  }

  @Test
  void sequentialRuntimesTransferTheRecordedSeamToTheSecondRuntime() {
    RecordingValueHolder holder = new RecordingValueHolder();
    DefaultRatchetRuntime firstRuntime =
        new RuntimeFixture().runtime(List.of(), List.of(holder.installation("runtime-a")));
    DefaultRatchetRuntime secondRuntime =
        new RuntimeFixture().runtime(List.of(), List.of(holder.installation("runtime-b")));

    firstRuntime.start();
    assertSame(firstRuntime, holder.owner());
    assertEquals("runtime-a", holder.value());

    firstRuntime.stop();
    assertNull(holder.owner());
    assertNull(holder.value());

    secondRuntime.start();
    assertSame(secondRuntime, holder.owner());
    assertEquals("runtime-b", holder.value());

    secondRuntime.stop();
  }

  @Test
  void repeatedStopIsSafeAndDoesNotRepeatShutdown() {
    RuntimeFixture fixture = new RuntimeFixture();
    RecordingInstallation installation = new RecordingInstallation();
    DefaultRatchetRuntime runtime = fixture.runtime(List.of(), List.of(installation));
    runtime.start();

    assertDoesNotThrow(
        () -> {
          runtime.stop();
          runtime.stop();
        });

    assertNull(installation.owner());
    assertEquals(1, installation.uninstallCalls());
    verify(fixture.drainController, times(1)).setDraining(true);
    verify(fixture.nodeIdentityProvider, times(1)).shutdown();
    verify(fixture.recurringRegistration, times(1)).cancel();
  }

  @Test
  void lifecycleHooksOnlyAdvanceWhenThePreviousPairedPhaseSucceeded() {
    SchedulerLifecycleHook successfulHook = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook beforeStartFailure = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook afterStartFailure = mock(SchedulerLifecycleHook.class);
    doThrow(new IllegalStateException("before-start failure"))
        .when(beforeStartFailure)
        .beforeStart();
    doThrow(new IllegalStateException("after-start failure")).when(afterStartFailure).afterStart();
    DefaultRatchetRuntime runtime =
        new RuntimeFixture()
            .runtime(List.of(successfulHook, beforeStartFailure, afterStartFailure), List.of());

    runtime.start();
    runtime.stop();

    verify(successfulHook).beforeStart();
    verify(successfulHook).afterStart();
    verify(successfulHook).beforeStop();
    verify(successfulHook).afterStop();

    verify(beforeStartFailure).beforeStart();
    verify(beforeStartFailure, never()).afterStart();
    verify(beforeStartFailure, never()).beforeStop();
    verify(beforeStartFailure, never()).afterStop();

    verify(afterStartFailure).beforeStart();
    verify(afterStartFailure).afterStart();
    verify(afterStartFailure, never()).beforeStop();
    verify(afterStartFailure, never()).afterStop();
  }

  private static void assertRuntimeServicesUntouched(RuntimeFixture fixture) {
    verifyNoInteractions(
        fixture.poller,
        fixture.recurringScheduler,
        fixture.orphanRecoveryTimer,
        fixture.batchRecoveryTimer,
        fixture.deadLetterService,
        fixture.jobArchivingService,
        fixture.logPurgeTimer,
        fixture.jobExecutionCoordinator,
        fixture.pollerWakeupListener,
        fixture.drainController,
        fixture.nodeIdentityProvider,
        fixture.scheduledExecutor);
  }

  private static final class RuntimeFixture {

    private final Poller poller = mock(Poller.class);
    private final RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    private final OrphanRecoveryTimer orphanRecoveryTimer = mock(OrphanRecoveryTimer.class);
    private final BatchRecoveryTimer batchRecoveryTimer = mock(BatchRecoveryTimer.class);
    private final DeadLetterService deadLetterService = mock(DeadLetterService.class);
    private final JobArchivingService jobArchivingService = mock(JobArchivingService.class);
    private final LogPurgeTimer logPurgeTimer = mock(LogPurgeTimer.class);
    private final JobExecutionCoordinator jobExecutionCoordinator =
        mock(JobExecutionCoordinator.class);
    private final PollerWakeupListener pollerWakeupListener = mock(PollerWakeupListener.class);
    private final DrainController drainController = mock(DrainController.class);
    private final DefaultNodeIdentityProvider nodeIdentityProvider =
        mock(DefaultNodeIdentityProvider.class);
    private final RecurringRegistration recurringRegistration = mock(RecurringRegistration.class);
    private final ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);

    private DefaultRatchetRuntime runtime(
        List<SchedulerLifecycleHook> hooks, List<RuntimeInstallation> installations) {
      return runtime(hooks, installations, null);
    }

    private DefaultRatchetRuntime runtimeWithScheduledWork(
        List<SchedulerLifecycleHook> hooks, List<RuntimeInstallation> installations) {
      return runtime(hooks, installations, scheduledExecutor);
    }

    private DefaultRatchetRuntime runtime(
        List<SchedulerLifecycleHook> hooks,
        List<RuntimeInstallation> installations,
        ScheduledExecutorService executor) {
      return new DefaultRatchetRuntime(
          poller,
          recurringScheduler,
          orphanRecoveryTimer,
          batchRecoveryTimer,
          deadLetterService,
          jobArchivingService,
          logPurgeTimer,
          jobExecutionCoordinator,
          pollerWakeupListener,
          drainController,
          null,
          nodeIdentityProvider,
          RatchetOptions.defaults(),
          hooks,
          executor == null ? null : () -> executor,
          recurringRegistration,
          installations);
    }
  }

  private static final class RecordingInstallation implements RuntimeInstallation {

    private Object owner;
    private int installCalls;
    private int uninstallCalls;

    @Override
    public synchronized void install(Object ownerToken) {
      installCalls++;
      if (owner != null && owner != ownerToken) {
        throw new IllegalStateException("runtime seam is already installed");
      }
      owner = ownerToken;
    }

    @Override
    public synchronized void uninstall(Object ownerToken) {
      uninstallCalls++;
      if (owner == ownerToken) {
        owner = null;
      }
    }

    private synchronized Object owner() {
      return owner;
    }

    private synchronized int installCalls() {
      return installCalls;
    }

    private synchronized int uninstallCalls() {
      return uninstallCalls;
    }
  }

  private static final class RecordingValueHolder {

    private Object owner;
    private String value;

    private RuntimeInstallation installation(String installedValue) {
      return new RuntimeInstallation() {
        @Override
        public void install(Object ownerToken) {
          synchronized (RecordingValueHolder.this) {
            if (owner != null && owner != ownerToken) {
              throw new IllegalStateException("runtime seam is already installed");
            }
            owner = ownerToken;
            value = installedValue;
          }
        }

        @Override
        public void uninstall(Object ownerToken) {
          synchronized (RecordingValueHolder.this) {
            if (owner == ownerToken) {
              owner = null;
              value = null;
            }
          }
        }
      };
    }

    private synchronized Object owner() {
      return owner;
    }

    private synchronized String value() {
      return value;
    }
  }
}
