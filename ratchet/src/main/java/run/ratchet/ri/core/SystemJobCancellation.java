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

import jakarta.enterprise.inject.Vetoed;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.ri.core.internal.AfterCommitActions;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;

/** System cancellation shared by public scheduling and background recovery. */
@Vetoed
public class SystemJobCancellation {
  private static final Logger log = Logger.getLogger(SystemJobCancellation.class);
  private final JobTerminalStore jobTerminalStore;
  private final WorkflowScheduler workflowScheduler;
  private final RatchetOptions options;
  private final InternalEventPublisher eventPublisher;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobCrudStore jobCrudStore;
  private final BatchService batchService;
  private final RecurringJobStore recurringJobStore;
  private final JobWakeupService wakeupService;
  private final MetricsCollector metricsCollector;
  private final Clock clock;
  private final AfterCommitRegistrar afterCommitRegistrar;

  public SystemJobCancellation(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobCrudStore jobCrudStore,
      BatchService batchService,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      MetricsCollector metricsCollector,
      Clock clock,
      AfterCommitRegistrar afterCommitRegistrar,
      JobTerminalStore jobTerminalStore,
      WorkflowScheduler workflowScheduler,
      RatchetOptions options) {
    this.jobTerminalStore = jobTerminalStore;
    this.workflowScheduler = workflowScheduler;
    this.options = options != null ? options : RatchetOptions.defaults();
    this.eventPublisher = eventPublisher;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobCrudStore = jobCrudStore;
    this.batchService = batchService;
    this.recurringJobStore = recurringJobStore;
    this.wakeupService = wakeupService;
    this.metricsCollector = metricsCollector;
    this.clock = clock;
    this.afterCommitRegistrar = afterCommitRegistrar;
  }

  /** Cancels without a caller authorization check. */
  @Transactional
  public boolean cancel(UUID jobId) {
    return cancelLoadedJob(jobId, jobCrudStore.findById(jobId).orElse(null), false);
  }

  boolean cancelLoadedJob(UUID jobId, JobEntity job, boolean holdKey) {
    // Try PENDING → CANCELED first (most common case)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PENDING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled pending job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.PENDING, job);
      cascadeCancel(job);
      return true;
    }

    // Try RUNNING → CANCELED. The scheduler publishes the durable state-transition event here so
    // observers do not lose cancellation if the worker dies before it notices the status flip.
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled running job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.RUNNING, job);
      cascadeCancel(job);
      return true;
    }

    // Try PAUSED → CANCELED
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PAUSED, JobStatus.CANCELED, null)) {
      log.debugf("Canceled paused job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.PAUSED, job);
      cascadeCancel(job);
      return true;
    }

    // Try WAITING → CANCELED (signal-waiting jobs can be canceled)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.WAITING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled waiting job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.WAITING, job);
      cascadeCancel(job);
      if (metricsCollector != null && job != null) {
        metricsCollector.signalCancelled(jobId, job.getPublicJobType(), job.getSignalKey());
      }
      return true;
    }

    // Recurring masters live in scheduler_recurring_job, not the executable queue. Fall through to
    // the dedicated SPI so callers holding a recurring master's UUID can cancel it the same way
    // they cancel an executable job. cancelRecurringAndArchive returns false if the id is unknown.
    if (recurringJobStore != null) {
      Optional<RecurringJobDefinition> def = recurringJobStore.getRecurring(jobId);
      if (def.isPresent()
          && recurringJobStore.cancelRecurringAndArchive(
              jobId, RecurringJobStore.ArchiveReason.CANCELED)) {
        log.debugf("Canceled recurring master %s", jobId);
        publishCancelledEventForRecurring(jobId, def.get());
        return true;
      }
    }

    log.debugf("Cannot cancel job %s — already in terminal state or not found", jobId);
    return false;
  }

  /**
   * Cancels the rest of a batch for a parent, or checks whether a cancelled child ended its batch.
   */
  private void cascadeCancel(JobEntity job) {
    if (batchService != null && job != null && job.getJobType() == JobExecutionType.BATCH_PARENT) {
      batchService.cancelBatch(job);
      UUID batchId = job.getId();
      AfterCommitActions.runOrFail(
          afterCommitRegistrar,
          "cancelled batch sweep",
          () -> {
            try {
              batchService.sweepCancelledBatch(batchId);
            } catch (RuntimeException ex) {
              log.warnf(
                  ex,
                  "Batch %s cancellation sweep failed; a child that became runnable during the"
                      + " cancel may still run",
                  batchId);
            }
          });
    }
    completeCancelledChildBatch(job);
  }

  private void completeCancelledChildBatch(JobEntity job) {
    if (batchService == null
        || job == null
        || job.getJobType() != JobExecutionType.BATCH_CHILD
        || job.getDependsOn() == null) {
      return;
    }
    UUID batchId = job.getDependsOn();
    AfterCommitActions.runOrFail(
        afterCommitRegistrar,
        "cancelled child batch completion",
        () -> {
          try {
            if (batchService.completeIfFinished(batchId) && wakeupService != null) {
              wakeupService.notify(JobPriority.NORMAL, true, null);
            }
          } catch (RuntimeException ex) {
            log.warnf(ex, "Batch %s completion deferred to recovery", batchId);
          }
        });
  }

  void publishCancelledEvent(UUID jobId, JobStatus previousStatus, JobEntity job) {
    Instant timestamp = effective().instant();
    JobCancelledEvent event =
        job == null
            // Race: job was deleted between CAS and our lookup. Fire a minimal event so observers
            // at least know the cancellation happened.
            ? new JobCancelledEvent(
                jobId, null, null, null, null, null, timestamp, previousStatus.name(), null)
            : new JobCancelledEvent(
                jobId,
                job.getBusinessKey(),
                job.getRecurringMasterId(),
                job.getPublicJobType(),
                job.getPriority(),
                job.getPickedBy(),
                timestamp,
                previousStatus.name(),
                null);
    // Defer publication until after the surrounding TX commits so a rollback does not produce a
    // spurious CANCELLED event. Falls back to immediate publication when no TX is active.
    publishAfterCommit(event);
  }

  private void publishCancelledEventForRecurring(UUID jobId, RecurringJobDefinition def) {
    JobStatus previousStatus = def.paused() ? JobStatus.PAUSED : JobStatus.PENDING;
    JobCancelledEvent event =
        new JobCancelledEvent(
            jobId,
            def.businessKey(),
            null,
            JobType.RECURRING,
            JobPriorityMapper.fromPersistedCode(def.priority()),
            null,
            effective().instant(),
            previousStatus.name(),
            null);
    publishAfterCommit(event);
  }

  private void publishAfterCommit(Object event) {
    AfterCommitActions.runOrFail(
        afterCommitRegistrar,
        event.getClass().getSimpleName(),
        () -> eventPublisher.publish(event));
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
