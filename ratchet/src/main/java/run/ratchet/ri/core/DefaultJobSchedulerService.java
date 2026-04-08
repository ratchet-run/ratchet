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
import run.ratchet.ri.payload.JobPayloadFactory;
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
 * Default implementation of {@link JobSchedulerService} for the ratchet reference implementation.
 *
 * <p>This class provides the core scheduling API, delegating to store SPIs and internal services
 * for persistence and coordination. It implements {@link JobSubmitter} so that it can be passed as
 * a method reference ({@code this::persistJob}) to {@link JobBuilder} instances.
 *
 * <p>Thread Safety: This service is thread-safe and can be safely used from multiple concurrent
 * contexts.
 *
 * @see JobSchedulerService
 * @see JobSubmitter
 * @see InternalEventPublisher
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

  // Required by CDI proxy
  protected DefaultJobSchedulerService() {
    this.eventPublisher = null;
    this.jobStatusStore = null;
    this.jobCrudStore = null;
    this.batchStore = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
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
      RecurringScheduler recurringScheduler) {
    this.eventPublisher = eventPublisher;
    this.jobStatusStore = jobStatusStore;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
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
        name, jobCrudStore, batchStore, workflowConditionStore, wakeupService);
  }

  @Override
  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return new DefaultStreamingBatchBuilder<>(
        name, jobCrudStore, batchStore, workflowConditionStore, wakeupService);
  }

  @Override
  public RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task) {
    return new DefaultRecurringJobBuilder(
        cron, zone, task, jobCrudStore, tagStore, recurringScheduler);
  }

  @Override
  @Transactional
  public JobHandle replace(
      long jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts) {
    // Load the existing job
    JobEntity existing =
        jobCrudStore
            .findById(jobId)
            .orElseThrow(
                () -> new IllegalArgumentException("Job not found for replacement: " + jobId));

    // Create the replacement job
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

    // Link old job to new via supersededBy regardless of cancel outcome
    existing.setSupersededBy(newHandle.id());
    jobCrudStore.save(existing);

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
    JobStatus current = jobCrudStore.getJobStatus(jobId);
    if (current == null) {
      log.debugf("Cannot pause job %s — not found", jobId);
      return false;
    }
    if (current == JobStatus.PAUSED) {
      log.debugf("Job %s is already paused", jobId);
      return true;
    }

    // Try PENDING → PAUSED (atomically sets paused_from_status)
    if (jobStatusStore.transitionToPaused(jobId, JobStatus.PENDING)) {
      log.debugf("Paused pending job %s", jobId);
      return true;
    }

    // Try FAILED → PAUSED (atomically sets paused_from_status)
    if (jobStatusStore.transitionToPaused(jobId, JobStatus.FAILED)) {
      log.debugf("Paused failed job %s", jobId);
      return true;
    }

    log.debugf(
        "Cannot pause job %s — current status %s is not pausable (only PENDING or FAILED)",
        jobId, current);
    return false;
  }

  @Override
  @Transactional
  public boolean resumeJob(long jobId) {
    JobStatus target = jobStatusStore.transitionFromPausedAtomic(jobId);
    if (target == null) {
      JobStatus current = jobCrudStore.getJobStatus(jobId);
      if (current == null) {
        log.debugf("Cannot resume job %s — not found", jobId);
      } else {
        log.debugf("Cannot resume job %s — not in PAUSED state (current: %s)", jobId, current);
      }
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
   * Persists a fully-configured job from a {@link JobBuilder}.
   *
   * <p>This is the {@link JobSubmitter} implementation. It creates a {@link JobEntity} from the
   * builder configuration, handles idempotency and business key checks, persists chain steps and
   * workflow branches, and triggers wakeup notifications as needed.
   *
   * @param builder the fully-configured job builder
   * @return a handle to the persisted job
   */
  @Override
  @Transactional
  public JobHandle submit(JobBuilder builder) {
    // Check idempotency key — return existing job if duplicate
    String idempotencyKey = builder.idempotencyKey();
    Optional<JobEntity> existingByKey = jobCrudStore.findByIdempotencyKey(idempotencyKey);
    if (existingByKey.isPresent()) {
      Long existingId = existingByKey.get().getId();
      log.debugf(
          "Duplicate idempotency key '%s', returning existing job %s", idempotencyKey, existingId);
      return () -> existingId;
    }

    // Check business key — reject if active job exists with same key
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

    // Create payload from lambda
    JobPayload payload = JobPayloadFactory.fromLambda(builder.task());

    // Build the job entity
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
      job.setOnSuccessPayload(JobPayloadFactory.fromLambda(builder.onSuccess()));
    }
    if (builder.onFailure() != null) {
      job.setOnFailurePayload(JobPayloadFactory.fromLambda(builder.onFailure()));
    }
    job.setMaxRetries(opts.maxRetries());
    job.setBackoffPolicy(opts.backoffPolicy());
    job.setBackoffParamMs((int) opts.backoffParam().toMillis());
    job.setTimeoutSec(opts.timeoutSec());
    if (!builder.params().isEmpty()) {
      job.setParams(builder.params());
    }

    // Persist the primary job
    JobEntity saved = jobCrudStore.save(job);
    Long jobId = saved.getId();

    // Insert tags
    List<String> tags = builder.tags();
    if (!tags.isEmpty()) {
      tagStore.insertTags(jobId, tags);
    }

    // Create chain step jobs (locked until predecessor completes)
    List<SerializableCheckedRunnable> chainTasks = builder.chainTasks();
    if (!chainTasks.isEmpty()) {
      createChainSteps(jobId, chainTasks, opts);
    }

    // Create workflow branches
    List<WorkflowBranch> branches = builder.workflowBranches();
    if (!branches.isEmpty()) {
      createWorkflowBranches(jobId, branches);
    }

    // Notify cluster for immediate wakeup if needed
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
      step.setPayload(JobPayloadFactory.fromLambda(chainTask));
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
      // Create a child job for the branch (locked until condition is evaluated)
      JobEntity branchJob = new JobEntity();
      branchJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
      branchJob.setStatus(JobStatus.PENDING);
      branchJob.setPriority(JobPriority.NORMAL);
      branchJob.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
      branchJob.setPayload(JobPayloadFactory.fromLambda(branch.task()));
      branchJob.setIdempotencyKey(UUID.randomUUID().toString());
      branchJob.setDependsOn(parentId);
      JobEntity savedBranch = jobCrudStore.save(branchJob);

      // Create the workflow condition record
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
}
