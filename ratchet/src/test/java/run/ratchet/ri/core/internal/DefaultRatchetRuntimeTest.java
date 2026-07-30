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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;

class DefaultRatchetRuntimeTest {

  @Test
  void schemaFailureReleasesInstalledSeamSoFreshRuntimeCanStart() {
    RecordingInstallation installation = new RecordingInstallation();
    RuntimeCollaborators failedCollaborators = new RuntimeCollaborators();
    DefaultNodeIdentityProvider failedNodeIdentityProvider =
        mock(DefaultNodeIdentityProvider.class);
    SchemaInitializationException startupFailure =
        new SchemaInitializationException("schema not ready");
    DefaultRatchetRuntime failedRuntime =
        failedCollaborators.runtime(
            failedNodeIdentityProvider,
            List.of(failingBeforeStartHook(startupFailure)),
            List.of(installation));

    SchemaInitializationException thrown =
        assertThrows(SchemaInitializationException.class, failedRuntime::start);

    assertSame(startupFailure, thrown);
    assertNull(installation.owner());
    assertEquals(1, installation.installCalls);
    assertEquals(1, installation.uninstallCalls);
    verify(failedNodeIdentityProvider, never()).init();
    verify(failedCollaborators.recurringRegistration, never()).register();
    verify(failedCollaborators.recurringRegistration).cancel();
    failedCollaborators.verifyStartupWorkWasNotReached();

    RuntimeCollaborators freshCollaborators = new RuntimeCollaborators();
    DefaultRatchetRuntime freshRuntime =
        freshCollaborators.runtime(
            mock(NodeIdentityProvider.class), List.of(), List.of(installation));

    freshRuntime.start();

    assertSame(freshRuntime, installation.owner());
    assertEquals(2, installation.installCalls);
    verify(freshCollaborators.recurringRegistration).register();
    verify(freshCollaborators.pollerWakeupListener).init();

    freshRuntime.stop();

    assertNull(installation.owner());
    assertEquals(2, installation.uninstallCalls);
  }

  @Test
  void rollbackSuppressesUninstallFailureWithoutReplacingSchemaFailure() {
    AtomicReference<Object> holder = new AtomicReference<>();
    RuntimeException uninstallFailure = new IllegalStateException("uninstall failed");
    RuntimeInstallation installation =
        new RuntimeInstallation() {
          @Override
          public void install(Object ownerToken) {
            if (!holder.compareAndSet(null, ownerToken)) {
              throw new IllegalStateException("runtime seam already owned");
            }
          }

          @Override
          public void uninstall(Object ownerToken) {
            holder.compareAndSet(ownerToken, null);
            throw uninstallFailure;
          }
        };
    RuntimeCollaborators collaborators = new RuntimeCollaborators();
    DefaultNodeIdentityProvider nodeIdentityProvider = mock(DefaultNodeIdentityProvider.class);
    SchemaInitializationException startupFailure =
        new SchemaInitializationException("schema not ready");
    DefaultRatchetRuntime runtime =
        collaborators.runtime(
            nodeIdentityProvider,
            List.of(failingBeforeStartHook(startupFailure)),
            List.of(installation));

    SchemaInitializationException thrown =
        assertThrows(SchemaInitializationException.class, runtime::start);

    assertSame(startupFailure, thrown);
    assertNull(holder.get());
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(uninstallFailure, thrown.getSuppressed()[0]);
    verify(nodeIdentityProvider, never()).init();
    verify(collaborators.recurringRegistration, never()).register();
    verify(collaborators.recurringRegistration).cancel();
    collaborators.verifyStartupWorkWasNotReached();
  }

  @Test
  void normalStopCancelsRecurringRegistrationBeforeShutdownWork() {
    RuntimeCollaborators collaborators = new RuntimeCollaborators();
    RuntimeInstallation installation = mock(RuntimeInstallation.class);
    SchedulerLifecycleHook hook = mock(SchedulerLifecycleHook.class);
    DefaultRatchetRuntime runtime =
        collaborators.runtime(
            mock(NodeIdentityProvider.class), List.of(hook), List.of(installation));

    runtime.start();
    runtime.stop();

    InOrder lifecycle =
        inOrder(
            collaborators.recurringRegistration, hook, collaborators.drainController, installation);
    lifecycle.verify(hook).beforeStart();
    lifecycle.verify(collaborators.recurringRegistration).register();
    lifecycle.verify(hook).afterStart();
    lifecycle.verify(collaborators.recurringRegistration).cancel();
    lifecycle.verify(hook).beforeStop();
    lifecycle.verify(collaborators.drainController).setDraining(true);
    lifecycle.verify(hook).afterStop();
    lifecycle.verify(installation).uninstall(runtime);
  }

  @Test
  void startupFailureAfterRecurringRegistrationCancelsBeforeUninstallingSeams() {
    RuntimeCollaborators collaborators = new RuntimeCollaborators();
    RuntimeInstallation installation = mock(RuntimeInstallation.class);
    IllegalStateException startupFailure = new IllegalStateException("listener failed");
    doThrow(startupFailure).when(collaborators.pollerWakeupListener).init();
    DefaultRatchetRuntime runtime =
        collaborators.runtime(mock(NodeIdentityProvider.class), List.of(), List.of(installation));

    IllegalStateException thrown = assertThrows(IllegalStateException.class, runtime::start);

    assertSame(startupFailure, thrown);
    InOrder unwind =
        inOrder(
            collaborators.recurringRegistration, collaborators.pollerWakeupListener, installation);
    unwind.verify(collaborators.recurringRegistration).register();
    unwind.verify(collaborators.pollerWakeupListener).init();
    unwind.verify(collaborators.recurringRegistration).cancel();
    unwind.verify(installation).uninstall(runtime);
  }

  private static SchedulerLifecycleHook failingBeforeStartHook(
      SchemaInitializationException failure) {
    return new SchedulerLifecycleHook() {
      @Override
      public void beforeStart() {
        throw failure;
      }
    };
  }

  private static final class RecordingInstallation implements RuntimeInstallation {

    private final AtomicReference<Object> holder = new AtomicReference<>();
    private int installCalls;
    private int uninstallCalls;

    @Override
    public void install(Object ownerToken) {
      if (!holder.compareAndSet(null, ownerToken)) {
        throw new IllegalStateException("runtime seam already owned");
      }
      installCalls++;
    }

    @Override
    public void uninstall(Object ownerToken) {
      if (holder.compareAndSet(ownerToken, null)) {
        uninstallCalls++;
      }
    }

    private Object owner() {
      return holder.get();
    }
  }

  private static final class RuntimeCollaborators {

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
    private final RatchetOptions options = mock(RatchetOptions.class);
    private final RecurringRegistration recurringRegistration = mock(RecurringRegistration.class);

    @SuppressWarnings("unchecked")
    private final Supplier<ScheduledExecutorService> scheduledExecutorSupplier =
        mock(Supplier.class);

    private DefaultRatchetRuntime runtime(
        NodeIdentityProvider nodeIdentityProvider,
        List<SchedulerLifecycleHook> lifecycleHooks,
        List<RuntimeInstallation> installations) {
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
          options,
          lifecycleHooks,
          scheduledExecutorSupplier,
          recurringRegistration,
          installations);
    }

    private void verifyStartupWorkWasNotReached() {
      verifyNoInteractions(
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
          options,
          scheduledExecutorSupplier);
    }
  }
}
