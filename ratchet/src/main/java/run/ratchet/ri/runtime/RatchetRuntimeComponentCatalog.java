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
package run.ratchet.ri.runtime;

import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.util.List;
import java.util.function.Supplier;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.CdiBeanResolver;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultRatchetRuntime;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringMethodRegistrar;
import run.ratchet.ri.core.internal.RecurringRegistration;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Catalog of Ratchet components eligible for container-managed construction.
 *
 * <p>A portable component is catalogable only if its selected constructor takes plain
 * bean-reference types — no {@code jakarta.enterprise.inject.Instance<T>} or other CDI-only
 * wrapper. Container adapter descriptors are retained so another container can filter and replace
 * them with its native adapter. Other components with {@code Instance<T>} constructors ({@code
 * BatchService}, {@code WorkflowScheduler}, etc.) join the catalog only after their seams are
 * repaired in PRs 5-7.
 */
public final class RatchetRuntimeComponentCatalog {

  private static final List<RatchetComponentDescriptor> COMPONENTS =
      List.of(
          new RatchetComponentDescriptor(JakartaAfterCommitRegistrar.class, List.of(), true, false),
          new RatchetComponentDescriptor(
              CdiBeanResolver.class, List.of(Instance.class), true, false),
          new RatchetComponentDescriptor(
              RecurringMethodInvoker.class,
              List.of(BeanResolver.class, ClassPolicy.class),
              true,
              false),
          new RatchetComponentDescriptor(
              RecurringRegistrationState.class,
              List.of(RatchetOptions.class, Clock.class),
              true,
              false),
          new RatchetComponentDescriptor(
              RecurringMethodRegistrar.class,
              List.of(
                  run.ratchet.api.JobSchedulerService.class,
                  RecurringAnnotationMaintenanceService.class,
                  RecurringMethodDiscovery.class,
                  RecurringMethodInvoker.class,
                  StartupCoordinator.class,
                  RecurringRegistrationState.class,
                  RatchetOptions.class,
                  ExecutorProvider.class,
                  JobStore.class,
                  Clock.class),
              true,
              false),
          new RatchetComponentDescriptor(
              JobStateManager.class,
              List.of(JobBatchStatusStore.class, NodeIdentityProvider.class),
              true,
              true),
          new RatchetComponentDescriptor(
              DeadLetterService.class,
              List.of(
                  ExecutorProvider.class,
                  JobBulkStore.class,
                  JobTerminalStore.class,
                  SingletonLeaseService.class,
                  InternalEventPublisher.class,
                  ErrorSanitizer.class,
                  Clock.class,
                  AfterCommitRegistrar.class),
              true,
              true),
          new RatchetComponentDescriptor(
              PostExecutionHandler.class,
              List.of(
                  BatchService.class,
                  WorkflowScheduler.class,
                  DeadLetterService.class,
                  PollerScheduler.class),
              true,
              true),
          new RatchetComponentDescriptor(
              DefaultRatchetRuntime.class,
              List.of(
                  Poller.class,
                  RecurringScheduler.class,
                  OrphanRecoveryTimer.class,
                  BatchRecoveryTimer.class,
                  DeadLetterService.class,
                  JobArchivingService.class,
                  LogPurgeTimer.class,
                  JobExecutionCoordinator.class,
                  PollerWakeupListener.class,
                  DrainController.class,
                  ClusterCoordinator.class,
                  NodeIdentityProvider.class,
                  RatchetOptions.class,
                  List.class,
                  Supplier.class,
                  RecurringRegistration.class,
                  List.class),
              true,
              false));

  private RatchetRuntimeComponentCatalog() {}

  /** Returns the deterministic, immutable component catalog. */
  public static List<RatchetComponentDescriptor> components() {
    return COMPONENTS;
  }
}
