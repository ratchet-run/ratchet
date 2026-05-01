package run.ratchet.ri.core;

import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/** Core scheduling API implementation. Delegates builder persistence to a CDI-managed service. */
@ApplicationScoped
public class DefaultJobSchedulerService
    implements JobSchedulerService, RecurringAnnotationMaintenanceService {

  private static final Logger log = Logger.getLogger(DefaultJobSchedulerService.class);

  private final InternalEventPublisher eventPublisher;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobPauseStore jobPauseStore;
  private final JobRetryStore jobRetryStore;
  private final JobTerminalStore jobTerminalStore;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final JobWakeupService wakeupService;
  private final RecurringScheduler recurringScheduler;
  private final JobInvocationResolver jobInvocationResolver;
  private final DefaultJobCreationService jobCreationService;

  @Resource private TransactionSynchronizationRegistry txRegistry;

  protected DefaultJobSchedulerService() {
    this.eventPublisher = null;
    this.jobBatchStatusStore = null;
    this.jobPauseStore = null;
    this.jobRetryStore = null;
    this.jobTerminalStore = null;
    this.jobCrudStore = null;
    this.batchStore = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
    this.jobInvocationResolver = null;
    this.jobCreationService = null;
  }

  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler) {
    this(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            wakeupService,
            recurringScheduler));
  }

  @Inject
  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      DefaultJobCreationService jobCreationService) {
    this.eventPublisher = eventPublisher;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobPauseStore = jobPauseStore;
    this.jobRetryStore = jobRetryStore;
    this.jobTerminalStore = jobTerminalStore;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
    this.jobInvocationResolver = jobInvocationResolver;
    this.jobCreationService = jobCreationService;
  }

  @Override
  @Transactional
  public boolean cancelJob(UUID jobId) {
    // Try PENDING → CANCELED first (most common case)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PENDING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled pending job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.PENDING);
      return true;
    }

    // Try RUNNING → CANCELED (the executor will publish JobCancelledEvent itself when the
    // running task observes the status flip — see JobTask. We do NOT publish here for the
    // RUNNING path to avoid duplicate events.)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled running job %s", jobId);
      return true;
    }

    // Try PAUSED → CANCELED
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PAUSED, JobStatus.CANCELED, null)) {
      log.debugf("Canceled paused job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.PAUSED);
      return true;
    }

    log.debugf("Cannot cancel job %s — already in terminal state or not found", jobId);
    return false;
  }

  /**
   * Publishes a {@link JobCancelledEvent} for a job that was cancelled outside the executor (i.e.,
   * from PENDING or PAUSED state). The RUNNING path publishes its own event from within {@code
   * JobTask} when the running task observes the status flip; we skip publication there to avoid
   * duplicate events. Loads the entity to populate businessKey / jobType / priority / nodeId on the
   * event so downstream observers (audit logs, monitoring) see the same shape they get for
   * running-cancellations.
   */
  private void publishCancelledEvent(UUID jobId, JobStatus previousStatus) {
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    JobCancelledEvent event =
        job == null
            // Race: job was deleted between CAS and our lookup. Fire a minimal event so observers
            // at least know the cancellation happened.
            ? new JobCancelledEvent(jobId, null, null, null, null, previousStatus.name(), null)
            : new JobCancelledEvent(
                jobId,
                job.getBusinessKey(),
                job.getPublicJobType(),
                job.getPriority(),
                job.getPickedBy(),
                previousStatus.name(),
                null);
    // Defer publication until after the surrounding TX commits so a rollback does not produce a
    // spurious CANCELLED event. Falls back to immediate publication when no TX is active.
    if (!registerAfterCommit(() -> eventPublisher.publish(event))) {
      eventPublisher.publish(event);
    }
  }

  private boolean registerAfterCommit(Runnable action) {
    if (txRegistry == null) {
      return false;
    }
    try {
      if (txRegistry.getTransactionStatus() != Status.STATUS_ACTIVE) {
        return false;
      }
      txRegistry.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {}

            @Override
            public void afterCompletion(int status) {
              if (status == Status.STATUS_COMMITTED) {
                action.run();
              }
            }
          });
      return true;
    } catch (Exception e) {
      log.warnf(
          "After-commit event registration failed; publishing immediately: %s", e.getMessage());
      return false;
    }
  }

  @Override
  public void addEventListener(Consumer<Object> listener) {
    eventPublisher.addListener(listener);
  }

  @Override
  public void removeEventListener(Consumer<Object> listener) {
    eventPublisher.removeListener(listener);
  }

  @Override
  public JobBuilder enqueue(SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, Duration.ZERO);
  }

  @Override
  public JobHandle enqueueNow(SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, Duration.ZERO).immediate().submit();
  }

  @Override
  public JobBuilder schedule(Duration delay, SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, delay);
  }

  @Override
  public BatchBuilder enqueueBatch(String name) {
    return new DefaultBatchBuilder(name, jobCreationService, jobInvocationResolver);
  }

  @Override
  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return new DefaultStreamingBatchBuilder<>(name, jobCreationService);
  }

  @Override
  public RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task) {
    return new DefaultRecurringJobBuilder(cron, zone, task, jobCreationService);
  }

  @Override
  @Transactional
  public JobHandle replace(
      UUID jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts) {
    // Fail fast if the job doesn't exist — don't create an orphaned replacement below.
    // We intentionally discard the loaded entity: the final save() after the CAS block reloads a
    // fresh snapshot whose version is post-CAS. See the block comment on that reload for the
    // stale-version rationale.
    jobCrudStore
        .findById(jobId)
        .orElseThrow(() -> new IllegalArgumentException("Job not found for replacement: " + jobId));

    JobBuilder builder = DefaultJobBuilder.create(jobCreationService, newTask, delay);
    if (opts != null) {
      builder
          .withPriority(opts.priority())
          .withMaxRetries(opts.maxRetries())
          .withBackoff(opts.backoffPolicy(), opts.backoffParam())
          .withTimeout(Duration.ofSeconds(opts.timeoutSec()));
    }
    JobHandle newHandle = builder.submit();

    // Cancel the old job using CAS to avoid conflicting with concurrent executors.
    // Try each cancellable state in order; if all fail, the job is already terminal.
    boolean canceled =
        jobBatchStatusStore.compareAndSwapStatus(jobId, JobStatus.PENDING, JobStatus.CANCELED, null)
            || jobBatchStatusStore.compareAndSwapStatus(
                jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)
            || jobBatchStatusStore.compareAndSwapStatus(
                jobId, JobStatus.PAUSED, JobStatus.CANCELED, null);

    // Reload after CAS: compareAndSwapStatus bumps the optimistic-lock version; writing the
    // pre-CAS entity would throw. Not routed through OptimisticLockRetry because the intent is
    // to annotate a terminal CANCELED row with supersededBy as an audit trail.
    JobEntity fresh =
        jobCrudStore
            .findById(jobId)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Job " + jobId + " vanished mid-replace (deleted between load and save)"));
    fresh.setSupersededBy(newHandle.id());
    jobCrudStore.save(fresh);

    if (canceled) {
      log.infof("Replaced job %s with new job %s", jobId, newHandle.id());
    } else {
      log.infof(
          "Replaced job %s with new job %s (old job already in terminal state)",
          jobId, newHandle.id());
    }
    return newHandle;
  }

  @Override
  @Transactional
  public boolean pauseJob(UUID jobId) {
    // Idempotent: already paused is a no-op success
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.debugf("Cannot pause job %s — not found", jobId);
      return false;
    }
    JobStatus current = job.getStatus();
    if (current == JobStatus.PAUSED) {
      log.debugf("Job %s is already paused", jobId);
      return true;
    }

    // Recurring masters use the rec_status shim (cold-only) — no hot row.
    if (job.getJobType() == JobExecutionType.RECURRING) {
      if (jobPauseStore.pauseRecurring(jobId)) {
        log.debugf("Paused recurring master %s", jobId);
        return true;
      }
      log.debugf("Cannot pause recurring master %s — current rec_status %s", jobId, current);
      return false;
    }

    // Executable jobs: try PENDING → PAUSED on hot.
    if (jobPauseStore.transitionToPaused(jobId, JobStatus.PENDING)) {
      log.debugf("Paused pending job %s", jobId);
      return true;
    }

    // Post hot/cold-split: pause-of-FAILED is no longer supported. FAILED is terminal-only and
    // has no hot row, so paused_from_status has nowhere to live. transitionToPaused returns
    // false for terminal expected statuses.
    log.debugf(
        "Cannot pause job %s — current status %s is not pausable (only PENDING is eligible)",
        jobId, current);
    return false;
  }

  @Override
  @Transactional
  public boolean resumeJob(UUID jobId) {
    // Recurring masters: rec_status 'A' → 'P'.
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.debugf("Cannot resume job %s — not found", jobId);
      return false;
    }
    if (job.getJobType() == JobExecutionType.RECURRING) {
      if (jobPauseStore.resumeRecurring(jobId)) {
        log.debugf("Resumed recurring master %s", jobId);
        recurringScheduler.kick();
        return true;
      }
      log.debugf("Cannot resume recurring master %s — not paused", jobId);
      return false;
    }

    JobStatus target = jobPauseStore.transitionFromPausedAtomic(jobId);
    if (target == null) {
      log.debugf(
          "Cannot resume job %s — not in PAUSED state (current: %s)", jobId, job.getStatus());
      return false;
    }
    log.debugf("Resumed job %s to %s", jobId, target);
    if (target == JobStatus.PENDING) {
      recurringScheduler.kick();
    }
    return true;
  }

  @Override
  @Transactional
  public boolean retryJob(UUID jobId) {
    if (jobRetryStore.resetFailedToPending(jobId)) {
      log.debugf("Retried failed job %s — reset to PENDING", jobId);
      return true;
    }

    JobStatus current = jobCrudStore.getJobStatus(jobId);
    if (current == null) {
      log.debugf("Cannot retry job %s — not found", jobId);
    } else {
      log.debugf("Cannot retry job %s — not in FAILED state (current: %s)", jobId, current);
    }
    return false;
  }

  @Override
  @Transactional
  public int cancelRecurringJobsByTag(String tag) {
    return jobBatchStatusStore.cancelRecurringJobsByTag(tag);
  }

  @Override
  @Transactional
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return jobBatchStatusStore.cancelRecurringJobByBusinessKey(businessKey);
  }

  @Transactional
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return jobBatchStatusStore.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }
}
