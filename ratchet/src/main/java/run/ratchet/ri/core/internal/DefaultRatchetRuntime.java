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

import com.cronutils.model.Cron;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.runtime.RatchetRuntime;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;

/** Default container-neutral orchestration of the Ratchet runtime lifecycle. */
public final class DefaultRatchetRuntime implements RatchetRuntime {

  private static final Logger log = Logger.getLogger(DefaultRatchetRuntime.class);

  private final Poller poller;
  private final RecurringScheduler recurringScheduler;
  private final OrphanRecoveryTimer orphanRecoveryTimer;
  private final BatchRecoveryTimer batchRecoveryTimer;
  private final DeadLetterService deadLetterService;
  private final JobArchivingService jobArchivingService;
  private final LogPurgeTimer logPurgeTimer;
  private final JobExecutionCoordinator jobExecutionCoordinator;
  private final PollerWakeupListener pollerWakeupListener;
  private final DrainController drainController;
  private final ClusterCoordinator clusterCoordinator;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final RatchetOptions options;
  private final List<SchedulerLifecycleHook> lifecycleHooks;
  private final Supplier<ScheduledExecutorService> scheduledExecutorSupplier;
  private final RecurringRegistration recurringRegistration;
  private final List<RuntimeInstallation> installations;

  private State state = State.NEW;
  private Progress progress;

  /**
   * Creates a runtime from plain collaborator references suitable for reflective container
   * construction.
   */
  public DefaultRatchetRuntime(
      Poller poller,
      RecurringScheduler recurringScheduler,
      OrphanRecoveryTimer orphanRecoveryTimer,
      BatchRecoveryTimer batchRecoveryTimer,
      DeadLetterService deadLetterService,
      JobArchivingService jobArchivingService,
      LogPurgeTimer logPurgeTimer,
      JobExecutionCoordinator jobExecutionCoordinator,
      PollerWakeupListener pollerWakeupListener,
      DrainController drainController,
      ClusterCoordinator clusterCoordinator,
      NodeIdentityProvider nodeIdentityProvider,
      RatchetOptions options,
      List<SchedulerLifecycleHook> lifecycleHooks,
      Supplier<ScheduledExecutorService> scheduledExecutorSupplier,
      RecurringRegistration recurringRegistration,
      List<RuntimeInstallation> installations) {
    this.poller = Objects.requireNonNull(poller, "poller");
    this.recurringScheduler = Objects.requireNonNull(recurringScheduler, "recurringScheduler");
    this.orphanRecoveryTimer = Objects.requireNonNull(orphanRecoveryTimer, "orphanRecoveryTimer");
    this.batchRecoveryTimer = Objects.requireNonNull(batchRecoveryTimer, "batchRecoveryTimer");
    this.deadLetterService = Objects.requireNonNull(deadLetterService, "deadLetterService");
    this.jobArchivingService = Objects.requireNonNull(jobArchivingService, "jobArchivingService");
    this.logPurgeTimer = Objects.requireNonNull(logPurgeTimer, "logPurgeTimer");
    this.jobExecutionCoordinator =
        Objects.requireNonNull(jobExecutionCoordinator, "jobExecutionCoordinator");
    this.pollerWakeupListener =
        Objects.requireNonNull(pollerWakeupListener, "pollerWakeupListener");
    this.drainController = Objects.requireNonNull(drainController, "drainController");
    this.clusterCoordinator = clusterCoordinator;
    this.nodeIdentityProvider =
        Objects.requireNonNull(nodeIdentityProvider, "nodeIdentityProvider");
    this.options = Objects.requireNonNull(options, "options");
    List<SchedulerLifecycleHook> sortedHooks =
        new ArrayList<>(Objects.requireNonNull(lifecycleHooks, "lifecycleHooks"));
    sortedHooks.sort(SchedulerLifecycleHookOrder.comparator());
    this.lifecycleHooks = List.copyOf(sortedHooks);
    this.scheduledExecutorSupplier = scheduledExecutorSupplier;
    this.recurringRegistration =
        Objects.requireNonNull(recurringRegistration, "recurringRegistration");
    this.installations = List.copyOf(Objects.requireNonNull(installations, "installations"));
  }

  @Override
  public synchronized void start() {
    if (state != State.NEW) {
      return;
    }

    Progress attempt = new Progress();
    log.info("Ratchet starting");
    try {
      installRuntimeSeams(attempt);
      List<SchedulerLifecycleHook> beforeStartSucceeded =
          notifyHooks("beforeStart", lifecycleHooks, SchedulerLifecycleHook::beforeStart, true);

      recurringRegistration.register();

      initializeDefaultNodeIdentityProvider(attempt);

      ScheduledExecutorService scheduledExecutor = resolveScheduledExecutorForStartup();
      if (scheduledExecutor != null) {
        startScheduledServices(attempt, scheduledExecutor);
      }

      pollerWakeupListener.init();
      attempt.startedHooks =
          notifyHooks(
              "afterStart", beforeStartSucceeded, SchedulerLifecycleHook::afterStart, false);

      progress = attempt;
      state = State.STARTED;
      log.info("Ratchet started");
    } catch (RuntimeException | Error failure) {
      rollback(attempt, failure);
      state = State.NEW;
      throw failure;
    }
  }

  @Override
  public synchronized void stop() {
    if (state != State.STARTED) {
      return;
    }
    state = State.STOPPED;

    Progress completed = progress;
    log.info("Ratchet stopping");
    stopService("recurring registration", recurringRegistration::cancel);
    List<SchedulerLifecycleHook> beforeStopSucceeded =
        notifyHooks(
            "beforeStop", completed.startedHooks, SchedulerLifecycleHook::beforeStop, false);

    stopService("drain controller", () -> drainController.setDraining(true));
    if (completed.nodeInitialized
        && nodeIdentityProvider instanceof DefaultNodeIdentityProvider defaultProvider) {
      stopService("default node identity provider", defaultProvider::shutdown);
    }

    stopCompletedServices(completed);

    if (clusterCoordinator != null && !coordinatorManagedByLifecycleHooks()) {
      stopService("cluster coordinator", clusterCoordinator::close);
    }

    notifyHooks("afterStop", beforeStopSucceeded, SchedulerLifecycleHook::afterStop, false);
    uninstallRuntimeSeams(completed.installed);
    log.info("Ratchet stopped");
  }

  private void installRuntimeSeams(Progress attempt) {
    for (RuntimeInstallation installation : installations) {
      try {
        installation.install(this);
        attempt.installed.add(installation);
      } catch (RuntimeException | Error failure) {
        suppressUninstallFailure(installation, failure);
        throw failure;
      }
    }
  }

  private void initializeDefaultNodeIdentityProvider(Progress attempt) {
    if (nodeIdentityProvider instanceof DefaultNodeIdentityProvider defaultProvider) {
      defaultProvider.init();
      attempt.nodeInitialized = true;
    }
  }

  private ScheduledExecutorService resolveScheduledExecutorForStartup() {
    if (scheduledExecutorSupplier == null) {
      return null;
    }
    try {
      return scheduledExecutorSupplier.get();
    } catch (RuntimeException e) {
      RatchetOptions.ExecutionOptions execution = options.execution();
      log.errorf(
          e,
          "Managed scheduled executor unavailable during Ratchet startup; scheduled background"
              + " services are disabled: polling, recurring scheduling, orphan recovery, batch"
              + " recovery, DLQ purge, job archiving, log purge, and retry-buffer draining."
              + " Configure RatchetOptions.execution().scheduledExecutorJndi(...) to a valid"
              + " ManagedScheduledExecutorService (current scheduledExecutorJndi=%s,"
              + " jobExecutorJndi=%s, virtualExecutorJndi=%s).",
          execution.scheduledExecutorJndi(),
          execution.jobExecutorJndi(),
          execution.virtualExecutorJndi());
      return null;
    }
  }

  private void startScheduledServices(
      Progress attempt, ScheduledExecutorService scheduledExecutor) {
    recurringScheduler.configure(
        options.recurring().pollMs(),
        options.recurring().maxPollMs(),
        options.recurring().batchLimit());

    poller.init();
    attempt.pollerStarted = true;

    recurringScheduler.init();
    attempt.recurringSchedulerStarted = true;

    orphanRecoveryTimer.start(scheduledExecutor, options.node().orphanScanIntervalMinutes());
    attempt.orphanRecoveryTimerStarted = true;

    batchRecoveryTimer.start(scheduledExecutor);
    attempt.batchRecoveryTimerStarted = true;

    if (options.maintenance().dlqPurgeEnabled()) {
      Cron dlqCron = RecurringScheduler.PARSER.parse(options.maintenance().dlqPurgeCron());
      deadLetterService.init(options.maintenance().dlqPurgeDays(), dlqCron);
      attempt.deadLetterServiceStarted = true;
    }

    if (options.maintenance().jobArchiveEnabled()) {
      Cron archiveCron = RecurringScheduler.PARSER.parse(options.maintenance().jobArchiveCron());
      jobArchivingService.init(
          true,
          options.maintenance().jobRetentionDays(),
          options.maintenance().jobArchiveBatchSize(),
          archiveCron);
      attempt.jobArchivingServiceStarted = true;
    }

    if (options.maintenance().logPurgeEnabled()) {
      Cron logCron = RecurringScheduler.PARSER.parse(options.maintenance().logPurgeCron());
      logPurgeTimer.init(options.maintenance().logRetentionDays(), logCron);
      attempt.logPurgeTimerStarted = true;
    }

    jobExecutionCoordinator.initRetryBufferDrainer();
    attempt.jobExecutionCoordinatorStarted = true;
  }

  private void rollback(Progress attempt, Throwable failure) {
    rollbackAction(recurringRegistration::cancel, failure);

    List<SchedulerLifecycleHook> beforeStopSucceeded =
        notifyHooksForRollback(
            "beforeStop", attempt.startedHooks, SchedulerLifecycleHook::beforeStop, failure);

    rollbackService(
        attempt.jobExecutionCoordinatorStarted, jobExecutionCoordinator::shutdown, failure);
    rollbackService(attempt.logPurgeTimerStarted, logPurgeTimer::stop, failure);
    rollbackService(attempt.jobArchivingServiceStarted, jobArchivingService::stop, failure);
    rollbackService(attempt.deadLetterServiceStarted, deadLetterService::stop, failure);
    rollbackService(attempt.batchRecoveryTimerStarted, batchRecoveryTimer::stop, failure);
    rollbackService(attempt.orphanRecoveryTimerStarted, orphanRecoveryTimer::stop, failure);
    rollbackService(attempt.recurringSchedulerStarted, recurringScheduler::stop, failure);
    rollbackService(attempt.pollerStarted, poller::stop, failure);

    if (attempt.nodeInitialized
        && nodeIdentityProvider instanceof DefaultNodeIdentityProvider defaultProvider) {
      rollbackAction(defaultProvider::shutdown, failure);
    }

    notifyHooksForRollback(
        "afterStop", beforeStopSucceeded, SchedulerLifecycleHook::afterStop, failure);

    for (int index = attempt.installed.size() - 1; index >= 0; index--) {
      suppressUninstallFailure(attempt.installed.get(index), failure);
    }
  }

  private void stopCompletedServices(Progress completed) {
    if (completed.pollerStarted) {
      stopService("poller", poller::stop);
    }
    if (completed.recurringSchedulerStarted) {
      stopService("recurring scheduler", recurringScheduler::stop);
    }
    if (completed.orphanRecoveryTimerStarted) {
      stopService("orphan recovery timer", orphanRecoveryTimer::stop);
    }
    if (completed.batchRecoveryTimerStarted) {
      stopService("batch recovery timer", batchRecoveryTimer::stop);
    }
    if (completed.deadLetterServiceStarted) {
      stopService("dead letter service", deadLetterService::stop);
    }
    if (completed.jobArchivingServiceStarted) {
      stopService("job archiving service", jobArchivingService::stop);
    }
    if (completed.logPurgeTimerStarted) {
      stopService("log purge timer", logPurgeTimer::stop);
    }
    if (completed.jobExecutionCoordinatorStarted) {
      stopService("job execution coordinator", jobExecutionCoordinator::shutdown);
    }
  }

  private boolean coordinatorManagedByLifecycleHooks() {
    if (!(clusterCoordinator instanceof SchedulerLifecycleHook coordinatorHook)) {
      return false;
    }
    return lifecycleHooks.stream()
        .anyMatch(hook -> hook == coordinatorHook || hook.equals(coordinatorHook));
  }

  private void uninstallRuntimeSeams(List<RuntimeInstallation> installed) {
    for (int index = installed.size() - 1; index >= 0; index--) {
      RuntimeInstallation installation = installed.get(index);
      try {
        installation.uninstall(this);
      } catch (Exception e) {
        log.warnf(e, "Failed to uninstall a Ratchet runtime seam: %s", e.getMessage());
      }
    }
  }

  private void suppressUninstallFailure(
      RuntimeInstallation installation, Throwable originalFailure) {
    rollbackAction(() -> installation.uninstall(this), originalFailure);
  }

  private void rollbackService(
      boolean completed, Runnable rollbackAction, Throwable originalFailure) {
    if (completed) {
      rollbackAction(rollbackAction, originalFailure);
    }
  }

  private void rollbackAction(Runnable action, Throwable originalFailure) {
    try {
      action.run();
    } catch (Throwable unwindFailure) {
      if (unwindFailure != originalFailure) {
        originalFailure.addSuppressed(unwindFailure);
      }
    }
  }

  private List<SchedulerLifecycleHook> notifyHooksForRollback(
      String phase,
      List<SchedulerLifecycleHook> phaseHooks,
      Consumer<SchedulerLifecycleHook> callback,
      Throwable originalFailure) {
    List<SchedulerLifecycleHook> succeeded = new ArrayList<>(phaseHooks.size());
    for (SchedulerLifecycleHook hook : phaseHooks) {
      try {
        callback.accept(hook);
        succeeded.add(hook);
      } catch (Throwable unwindFailure) {
        if (unwindFailure != originalFailure) {
          originalFailure.addSuppressed(unwindFailure);
        }
      }
    }
    return List.copyOf(succeeded);
  }

  private List<SchedulerLifecycleHook> notifyHooks(
      String phase,
      List<SchedulerLifecycleHook> phaseHooks,
      Consumer<SchedulerLifecycleHook> callback,
      boolean abortOnSchemaFailure) {
    List<SchedulerLifecycleHook> succeeded = new ArrayList<>(phaseHooks.size());
    for (SchedulerLifecycleHook hook : phaseHooks) {
      try {
        callback.accept(hook);
        succeeded.add(hook);
      } catch (SchemaInitializationException e) {
        if (abortOnSchemaFailure) {
          log.errorf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
          throw e;
        }
        log.warnf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
      } catch (Exception e) {
        log.warnf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
      }
    }
    return List.copyOf(succeeded);
  }

  private void stopService(String name, Runnable stopAction) {
    try {
      stopAction.run();
    } catch (Exception e) {
      log.warnf(e, "Failed to stop %s: %s", name, e.getMessage());
    }
  }

  private enum State {
    NEW,
    STARTED,
    STOPPED
  }

  private static final class Progress {
    private final List<RuntimeInstallation> installed = new ArrayList<>();
    private List<SchedulerLifecycleHook> startedHooks = List.of();
    private boolean nodeInitialized;
    private boolean pollerStarted;
    private boolean recurringSchedulerStarted;
    private boolean orphanRecoveryTimerStarted;
    private boolean batchRecoveryTimerStarted;
    private boolean deadLetterServiceStarted;
    private boolean jobArchivingServiceStarted;
    private boolean logPurgeTimerStarted;
    private boolean jobExecutionCoordinatorStarted;
  }
}
