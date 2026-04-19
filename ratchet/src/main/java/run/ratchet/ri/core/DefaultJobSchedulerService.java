package run.ratchet.ri.core;

import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStatusStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * Core scheduling API implementation. Delegates to store SPIs for persistence and also implements
 * {@link JobSubmitter} so builders can call back into {@link #submit(JobBuilder)}.
 */
@ApplicationScoped
public class DefaultJobSchedulerService
    implements JobSchedulerService, JobSubmitter, RecurringAnnotationMaintenanceService {

  private static final Logger log = Logger.getLogger(DefaultJobSchedulerService.class);

  private final InternalEventPublisher eventPublisher;
  private final JobStatusStore jobStatusStore;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final JobWakeupService wakeupService;
  private final RecurringScheduler recurringScheduler;
  private final JobInvocationResolver jobInvocationResolver;

  protected DefaultJobSchedulerService() {
    this.eventPublisher = null;
    this.jobStatusStore = null;
    this.jobCrudStore = null;
    this.batchStore = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
    this.jobInvocationResolver = null;
  }

  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobStatusStore jobStatusStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler) {
    this(
        eventPublisher,
        jobStatusStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver());
  }

  @Inject
  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobStatusStore jobStatusStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver) {
    this.eventPublisher = eventPublisher;
    this.jobStatusStore = jobStatusStore;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
    this.jobInvocationResolver = jobInvocationResolver;
  }

  @Override
  @Transactional
  public boolean cancelJob(long jobId) {
    // Try PENDING → CANCELED first (most common case)
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PENDING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled pending job %s", jobId);
      return true;
    }

    // Try RUNNING → CANCELED (executor should check status before committing)
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled running job %s", jobId);
      return true;
    }

    // Try PAUSED → CANCELED
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PAUSED, JobStatus.CANCELED, null)) {
      log.debugf("Canceled paused job %s", jobId);
      return true;
    }

    log.debugf("Cannot cancel job %s — already in terminal state or not found", jobId);
    return false;
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
    return DefaultJobBuilder.create(this, task, Duration.ZERO);
  }

  @Override
  public JobHandle enqueueNow(SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(this, task, Duration.ZERO).immediate().submit();
  }

  @Override
  public JobBuilder schedule(Duration delay, SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(this, task, delay);
  }

  @Override
  public BatchBuilder enqueueBatch(String name) {
    return new DefaultBatchBuilder(
        name,
        jobCrudStore,
        jobStatusStore,
        batchStore,
        workflowConditionStore,
        wakeupService,
        jobInvocationResolver);
  }

  @Override
  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return new DefaultStreamingBatchBuilder<>(
        name,
        jobCrudStore,
        batchStore,
        workflowConditionStore,
        wakeupService,
        jobInvocationResolver);
  }

  @Override
  public RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task) {
    return new DefaultRecurringJobBuilder(
        cron, zone, task, jobCrudStore, tagStore, recurringScheduler, jobInvocationResolver);
  }

  @Override
  @Transactional
  public JobHandle replace(
      long jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts) {
    // Fail fast if the job doesn't exist — don't create an orphaned replacement below.
    // We intentionally discard the loaded entity: the final save() after the CAS block reloads a
    // fresh snapshot whose version is post-CAS. See the block comment on that reload for the
    // stale-version rationale.
    jobCrudStore
        .findById(jobId)
        .orElseThrow(() -> new IllegalArgumentException("Job not found for replacement: " + jobId));

    JobBuilder builder = DefaultJobBuilder.create(this, newTask, delay);
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
        jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PENDING, JobStatus.CANCELED, null)
            || jobStatusStore.compareAndSwapStatus(
                jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)
            || jobStatusStore.compareAndSwapStatus(
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
  public boolean pauseJob(long jobId) {
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
      if (jobStatusStore.pauseRecurring(jobId)) {
        log.debugf("Paused recurring master %s", jobId);
        return true;
      }
      log.debugf("Cannot pause recurring master %s — current rec_status %s", jobId, current);
      return false;
    }

    // Executable jobs: try PENDING → PAUSED on hot.
    if (jobStatusStore.transitionToPaused(jobId, JobStatus.PENDING)) {
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
  public boolean resumeJob(long jobId) {
    // Recurring masters: rec_status 'A' → 'P'.
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.debugf("Cannot resume job %s — not found", jobId);
      return false;
    }
    if (job.getJobType() == JobExecutionType.RECURRING) {
      if (jobStatusStore.resumeRecurring(jobId)) {
        log.debugf("Resumed recurring master %s", jobId);
        recurringScheduler.kick();
        return true;
      }
      log.debugf("Cannot resume recurring master %s — not paused", jobId);
      return false;
    }

    JobStatus target = jobStatusStore.transitionFromPausedAtomic(jobId);
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
  public boolean retryJob(long jobId) {
    if (jobStatusStore.resetFailedToPending(jobId)) {
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
    return jobStatusStore.cancelRecurringJobsByTag(tag);
  }

  @Override
  @Transactional
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return jobStatusStore.cancelRecurringJobByBusinessKey(businessKey);
  }

  @Transactional
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return jobStatusStore.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }

  /**
   * {@link JobSubmitter} implementation. Handles idempotency, persists chain/workflow steps, and
   * wakes the poller if needed.
   */
  @Override
  @Transactional
  public JobHandle submit(JobBuilder builder) {
    String idempotencyKey = builder.idempotencyKey();
    Optional<JobEntity> existingByKey = jobCrudStore.findByIdempotencyKey(idempotencyKey);
    if (existingByKey.isPresent()) {
      Long existingId = existingByKey.get().getId();
      log.debugf(
          "Duplicate idempotency key '%s', returning existing job %s", idempotencyKey, existingId);
      return () -> existingId;
    }

    String businessKey = builder.businessKey();
    if (businessKey != null) {
      Optional<JobEntity> activeByBk = jobCrudStore.findActiveByBusinessKey(businessKey);
      if (activeByBk.isPresent()) {
        throw new IllegalStateException(
            "Active job already exists with business key '"
                + businessKey
                + "' (jobId="
                + activeByBk.get().getId()
                + ")");
      }
    }

    JobPayload payload = payload(builder.task());

    JobOptions opts = builder.opts();
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(opts.priority());
    job.setScheduledTime(Instant.now().plus(builder.delay()));
    job.setPayload(payload);
    job.setIdempotencyKey(idempotencyKey);
    job.setBusinessKey(businessKey);
    job.setResourceName(builder.resourceName());
    if (builder.onSuccess() != null) {
      job.setOnSuccessPayload(payload(builder.onSuccess()));
    }
    if (builder.onFailure() != null) {
      job.setOnFailurePayload(payload(builder.onFailure()));
    }
    job.setMaxRetries(opts.maxRetries());
    job.setBackoffPolicy(opts.backoffPolicy());
    job.setBackoffParamMs((int) opts.backoffParam().toMillis());
    job.setTimeoutSec(opts.timeoutSec());
    if (!builder.params().isEmpty()) {
      job.setParams(builder.params());
    }

    JobEntity saved = jobCrudStore.save(job);
    Long jobId = saved.getId();

    List<String> tags = builder.tags();
    if (!tags.isEmpty()) {
      tagStore.insertTags(jobId, tags);
    }

    List<SerializableCheckedRunnable> chainTasks = builder.chainTasks();
    if (!chainTasks.isEmpty()) {
      createChainSteps(jobId, chainTasks, opts);
    }

    List<WorkflowBranch> branches = builder.workflowBranches();
    if (!branches.isEmpty()) {
      createWorkflowBranches(jobId, branches);
    }

    boolean shouldWakeup =
        builder.isImmediate()
            || opts.priority() == JobPriority.CRITICAL
            || builder.delay().isZero();
    if (shouldWakeup) {
      wakeupService.notify(opts.priority(), true);
    }

    log.debugf("Job submitted (id=%s, type=SINGLE, delay=%s)", jobId, builder.delay());
    return () -> jobId;
  }

  private void createChainSteps(
      Long predecessorId, List<SerializableCheckedRunnable> chainTasks, JobOptions opts) {
    Long prevId = predecessorId;
    for (SerializableCheckedRunnable chainTask : chainTasks) {
      JobEntity step = new JobEntity();
      step.setJobType(JobExecutionType.CHAIN_STEP);
      step.setStatus(JobStatus.PENDING);
      step.setPriority(opts.priority());
      step.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
      step.setPayload(payload(chainTask));
      step.setIdempotencyKey(UUID.randomUUID().toString());
      step.setDependsOn(prevId);
      step.setMaxRetries(opts.maxRetries());
      step.setBackoffPolicy(opts.backoffPolicy());
      step.setBackoffParamMs((int) opts.backoffParam().toMillis());
      step.setTimeoutSec(opts.timeoutSec());

      JobEntity savedStep = jobCrudStore.save(step);
      prevId = savedStep.getId();
    }
  }

  private void createWorkflowBranches(Long parentId, List<WorkflowBranch> branches) {
    for (WorkflowBranch branch : branches) {
      JobEntity branchJob = new JobEntity();
      branchJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
      branchJob.setStatus(JobStatus.PENDING);
      branchJob.setPriority(JobPriority.NORMAL);
      branchJob.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
      branchJob.setPayload(payload(branch.task()));
      branchJob.setIdempotencyKey(UUID.randomUUID().toString());
      branchJob.setDependsOn(parentId);
      JobEntity savedBranch = jobCrudStore.save(branchJob);

      WorkflowConditionEntity condition = new WorkflowConditionEntity();
      condition.setParentJobId(parentId);
      condition.setChildJobId(savedBranch.getId());
      condition.setConditionType(branch.condition().type());
      condition.setConditionPriority(branch.condition().priority());
      if (branch.condition().expression() != null) {
        condition.setConditionExpressionSerialized(branch.condition().expression());
      }
      workflowConditionStore.saveCondition(condition);
    }
  }

  private JobPayload payload(Serializable callback) {
    return JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback));
  }
}
