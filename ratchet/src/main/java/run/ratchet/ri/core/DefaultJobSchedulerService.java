package run.ratchet.ri.core;

import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.JobType;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.util.LambdaSerializer;
import run.ratchet.store.entity.JobEntity;
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
import java.util.logging.Logger;

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
@Transactional
public class DefaultJobSchedulerService implements JobSchedulerService, JobSubmitter {

  private static final Logger log = Logger.getLogger(DefaultJobSchedulerService.class.getName());

  private final InternalEventPublisher eventPublisher;
  private final JobStatusStore jobStatusStore;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final JobWakeupService wakeupService;
  private final LambdaSerializer lambdaSerializer;
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
    this.lambdaSerializer = null;
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
      LambdaSerializer lambdaSerializer,
      RecurringScheduler recurringScheduler) {
    this.eventPublisher = eventPublisher;
    this.jobStatusStore = jobStatusStore;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
    this.lambdaSerializer = lambdaSerializer;
    this.recurringScheduler = recurringScheduler;
  }

  @Override
  public boolean cancelJob(long jobId) {
    // Try PENDING → CANCELED first (most common case)
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PENDING, JobStatus.CANCELED, null)) {
      log.fine("Canceled pending job " + jobId);
      return true;
    }

    // Try RUNNING → CANCELED (executor should check status before committing)
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      log.fine("Canceled running job " + jobId);
      return true;
    }

    // Try PAUSED → CANCELED
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PAUSED, JobStatus.CANCELED, null)) {
      log.fine("Canceled paused job " + jobId);
      return true;
    }

    log.fine("Cannot cancel job " + jobId + " — already in terminal state or not found");
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
    return JobBuilder.create(this, task, Duration.ZERO);
  }

  @Override
  public JobHandle enqueueNow(SerializableCheckedRunnable task) {
    return JobBuilder.create(this, task, Duration.ZERO).immediate().submit();
  }

  @Override
  public JobBuilder schedule(Duration delay, SerializableCheckedRunnable task) {
    return JobBuilder.create(this, task, delay);
  }

  @Override
  public BatchBuilder enqueueBatch(String name) {
    return new DefaultBatchBuilder(
        name,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        lambdaSerializer,
        wakeupService);
  }

  @Override
  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return new DefaultStreamingBatchBuilder<>(
        name,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        lambdaSerializer,
        wakeupService);
  }

  @Override
  public RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task) {
    return new DefaultRecurringJobBuilder(
        cron, zone, task, jobCrudStore, tagStore, recurringScheduler);
  }

  @Override
  public JobHandle replace(
      long jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts) {
    // Load the existing job
    JobEntity existing =
        jobCrudStore
            .findById(jobId)
            .orElseThrow(
                () -> new IllegalArgumentException("Job not found for replacement: " + jobId));

    // Create the replacement job
    JobBuilder builder = JobBuilder.create(this, newTask, delay);
    if (opts != null) {
      builder
          .withPriority(opts.priority())
          .withMaxRetries(opts.maxRetries())
          .withBackoff(opts.backoffPolicy(), opts.backoffParam())
          .withTimeout(Duration.ofSeconds(opts.timeoutSec()));
    }
    JobHandle newHandle = builder.submit();

    // Link old job to new via supersededBy
    existing.setSupersededBy(newHandle.id());
    existing.setStatus(JobStatus.CANCELED);
    jobCrudStore.save(existing);

    log.info("Replaced job " + jobId + " with new job " + newHandle.id());
    return newHandle;
  }

  @Override
  public boolean pauseJob(long jobId) {
    // Idempotent: already paused is a no-op success
    JobStatus current = jobCrudStore.getJobStatus(jobId);
    if (current == null) {
      log.fine("Cannot pause job " + jobId + " — not found");
      return false;
    }
    if (current == JobStatus.PAUSED) {
      log.fine("Job " + jobId + " is already paused");
      return true;
    }

    // Try PENDING → PAUSED
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PENDING, JobStatus.PAUSED, null)) {
      // Record the previous status for resume
      jobCrudStore
          .findById(jobId)
          .ifPresent(
              job -> {
                job.setPausedFromStatus(JobStatus.PENDING);
                jobCrudStore.save(job);
              });
      log.fine("Paused pending job " + jobId);
      return true;
    }

    // Try FAILED → PAUSED
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.FAILED, JobStatus.PAUSED, null)) {
      jobCrudStore
          .findById(jobId)
          .ifPresent(
              job -> {
                job.setPausedFromStatus(JobStatus.FAILED);
                jobCrudStore.save(job);
              });
      log.fine("Paused failed job " + jobId);
      return true;
    }

    log.fine(
        "Cannot pause job "
            + jobId
            + " — current status "
            + current
            + " is not pausable (only PENDING or FAILED)");
    return false;
  }

  @Override
  public boolean resumeJob(long jobId) {
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.PAUSED, JobStatus.PENDING, null)) {
      log.fine("Resumed job " + jobId);
      // Kick the recurring scheduler in case this is a recurring/cron job
      recurringScheduler.kick();
      return true;
    }

    JobStatus current = jobCrudStore.getJobStatus(jobId);
    if (current == null) {
      log.fine("Cannot resume job " + jobId + " — not found");
    } else {
      log.fine("Cannot resume job " + jobId + " — not in PAUSED state (current: " + current + ")");
    }
    return false;
  }

  @Override
  public boolean retryJob(long jobId) {
    if (jobStatusStore.compareAndSwapStatus(jobId, JobStatus.FAILED, JobStatus.PENDING, null)) {
      jobCrudStore
          .findById(jobId)
          .ifPresent(
              job -> {
                job.setAttempts(0);
                job.setLastError(null);
                job.setScheduledTime(Instant.now());
                jobCrudStore.save(job);
              });
      log.fine("Retried failed job " + jobId + " — reset to PENDING");
      return true;
    }

    JobStatus current = jobCrudStore.getJobStatus(jobId);
    if (current == null) {
      log.fine("Cannot retry job " + jobId + " — not found");
    } else {
      log.fine("Cannot retry job " + jobId + " — not in FAILED state (current: " + current + ")");
    }
    return false;
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return jobStatusStore.cancelRecurringJobsByTag(tag);
  }

  @Override
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
  public JobHandle submit(JobBuilder builder) {
    // Check idempotency key — return existing job if duplicate
    String idempotencyKey = builder.idempotencyKey();
    Optional<JobEntity> existingByKey = jobCrudStore.findByIdempotencyKey(idempotencyKey);
    if (existingByKey.isPresent()) {
      Long existingId = existingByKey.get().getId();
      log.fine(
          "Duplicate idempotency key '"
              + idempotencyKey
              + "', returning existing job "
              + existingId);
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
    job.setJobType(JobType.SINGLE);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(opts.priority());
    job.setScheduledTime(Instant.now().plus(builder.delay()));
    job.setPayload(payload);
    job.setIdempotencyKey(idempotencyKey);
    job.setBusinessKey(businessKey);
    job.setResourceName(builder.resourceName());
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

    log.fine("Job submitted (id=" + jobId + ", type=SINGLE, delay=" + builder.delay() + ")");
    return () -> jobId;
  }

  private void createChainSteps(
      Long predecessorId, List<SerializableCheckedRunnable> chainTasks, JobOptions opts) {
    Long prevId = predecessorId;
    for (SerializableCheckedRunnable chainTask : chainTasks) {
      JobEntity step = new JobEntity();
      step.setJobType(JobType.CHAIN_STEP);
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
      branchJob.setJobType(JobType.WORKFLOW_BRANCH);
      branchJob.setStatus(JobStatus.PENDING);
      branchJob.setPriority(JobPriority.NORMAL);
      branchJob.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
      branchJob.setPayload(
          JobPayloadFactory.fromLambda((SerializableCheckedRunnable) branch.task()));
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
