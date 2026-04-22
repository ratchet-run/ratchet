package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;

/** CDI-managed persistence boundary for scheduler builders. */
@ApplicationScoped
public class DefaultJobCreationService
    implements JobSubmitter, BatchSubmitter, StreamingBatchSubmitter, RecurringJobSubmitter {

  private static final Logger log = Logger.getLogger(DefaultJobCreationService.class);

  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobTerminalStore jobTerminalStore;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final JobWakeupService wakeupService;
  private final RecurringScheduler recurringScheduler;
  private final JobInvocationResolver jobInvocationResolver;
  private final JobPayloadInputValidator payloadValidator;

  protected DefaultJobCreationService() {
    this.jobBatchStatusStore = null;
    this.jobTerminalStore = null;
    this.jobCrudStore = null;
    this.batchStore = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
    this.jobInvocationResolver = null;
    this.payloadValidator = null;
  }

  public DefaultJobCreationService(
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler) {
    this(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator());
  }

  @Inject
  public DefaultJobCreationService(
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      JobPayloadInputValidator payloadValidator) {
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobTerminalStore = jobTerminalStore;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
    this.jobInvocationResolver = jobInvocationResolver;
    this.payloadValidator = payloadValidator;
  }

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
    applyOptions(job, opts);
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

  @Override
  @Transactional
  public JobHandle submit(DefaultBatchBuilder builder) {
    JobEntity parent = newBatchParent();
    JobEntity savedParent = jobCrudStore.save(parent);
    Long parentId = savedParent.getId();

    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(builder.children().size());
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (builder.progressHook() != null) {
      batch.setProgressHook(payload(builder.progressHook()));
    }
    batchStore.saveBatch(batch);

    if (builder.children().isEmpty()) {
      completeEmptyBatch(parentId);
      log.infof(
          "Batch '%s' submitted with 0 children — completed immediately (id=%s)",
          builder.name(), parentId);
      return () -> parentId;
    }

    for (DefaultBatchBuilder.ChildSpec child : builder.children()) {
      JobEntity childJob = new JobEntity();
      childJob.setJobType(JobExecutionType.BATCH_CHILD);
      childJob.setStatus(JobStatus.PENDING);
      childJob.setPriority(JobPriority.NORMAL);
      childJob.setScheduledTime(Instant.now());
      childJob.setPayload(validate(child.payload()));
      childJob.setIdempotencyKey(UUID.randomUUID().toString());
      childJob.setDependsOn(parentId);
      jobCrudStore.save(childJob);
    }

    for (WorkflowBranch branch : builder.workflowBranches()) {
      createWorkflowBranch(parentId, branch);
    }

    wakeupService.notifyIfNeeded(JobExecutionType.BATCH_PARENT, JobPriority.NORMAL, Duration.ZERO);

    log.infof(
        "Batch '%s' submitted with %s children (id=%s)",
        builder.name(), builder.children().size(), parentId);
    return () -> parentId;
  }

  @Override
  @Transactional
  public <T extends Serializable> JobHandle submit(DefaultStreamingBatchBuilder<T> builder) {
    builder.validateReady();

    JobEntity parent = newBatchParent();
    JobEntity savedParent = jobCrudStore.save(parent);
    Long parentId = savedParent.getId();

    int totalItems = 0;
    int chunksInserted = 0;
    List<T> chunk = builder.newChunk();

    try {
      var iterator = builder.stream().iterator();
      while (iterator.hasNext()) {
        chunk.add(iterator.next());
        if (chunk.size() >= builder.chunkSize()) {
          totalItems += createStreamingChildJobs(parentId, builder, chunk);
          chunksInserted++;
          builder.invokeLocalProgressHook(parentId, totalItems, chunksInserted);
          chunk.clear();
        }
      }

      if (!chunk.isEmpty()) {
        totalItems += createStreamingChildJobs(parentId, builder, chunk);
        chunksInserted++;
        builder.invokeLocalProgressHook(parentId, totalItems, chunksInserted);
      }
    } finally {
      builder.stream().close();
    }

    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(totalItems);
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (builder.batchProgressHook() != null) {
      batch.setProgressHook(payload(builder.batchProgressHook()));
    }
    batchStore.saveBatch(batch);

    for (WorkflowBranch branch : builder.workflowBranches()) {
      createWorkflowBranch(parentId, branch);
    }

    wakeupService.notifyIfNeeded(JobExecutionType.BATCH_PARENT, JobPriority.NORMAL, Duration.ZERO);

    log.infof(
        "Streaming batch '%s' submitted with %s items (id=%s)",
        builder.name(), totalItems, parentId);
    return () -> parentId;
  }

  @Override
  @Transactional
  public JobHandle submit(DefaultRecurringJobBuilder builder) {
    Cron cron = RecurringScheduler.PARSER.parse(builder.cronExpr());
    cron.validate();

    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    ZonedDateTime now = ZonedDateTime.now(builder.zone());
    Instant nextFire =
        executionTime
            .nextExecution(now)
            .map(ZonedDateTime::toInstant)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Cron expression '"
                            + builder.cronExpr()
                            + "' has no future execution time"));

    JobOptions options = builder.options();
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.RECURRING);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(options.priority());
    job.setScheduledTime(Instant.now());
    job.setPayload(payload(builder.task()));
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setBusinessKey(builder.businessKey());
    job.setCronExpr(builder.cronExpr());
    job.setZoneId(builder.zone().getId());
    job.setNextFire(nextFire);
    applyOptions(job, options);

    JobEntity saved = jobCrudStore.save(job);

    if (!builder.tags().isEmpty()) {
      tagStore.insertTags(saved.getId(), builder.tags());
    }

    log.infof(
        "Recurring job submitted (id=%s, cron=%s, zone=%s, nextFire=%s)",
        saved.getId(), builder.cronExpr(), builder.zone(), nextFire);

    recurringScheduler.kick();
    return saved::getId;
  }

  private void applyOptions(JobEntity job, JobOptions opts) {
    job.setMaxRetries(opts.maxRetries());
    job.setBackoffPolicy(opts.backoffPolicy());
    job.setBackoffParamMs((int) opts.backoffParam().toMillis());
    job.setTimeoutSec(opts.timeoutSec());
  }

  private JobEntity newBatchParent() {
    JobEntity parent = new JobEntity();
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setStatus(JobStatus.PENDING);
    parent.setPriority(JobPriority.NORMAL);
    parent.setScheduledTime(Instant.now());
    parent.setPayload(validate(JobPayloadFactory.noop()));
    parent.setIdempotencyKey(UUID.randomUUID().toString());
    return parent;
  }

  private void completeEmptyBatch(Long parentId) {
    if (jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
      Instant now = Instant.now();
      jobTerminalStore.markJobSucceededMinimal(parentId, now, now, 0L, 0L);
    }
    batchStore.markBatchCompleteIfReady(parentId);
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
      applyOptions(step, opts);

      JobEntity savedStep = jobCrudStore.save(step);
      prevId = savedStep.getId();
    }
  }

  private <T extends Serializable> int createStreamingChildJobs(
      Long parentId, DefaultStreamingBatchBuilder<T> builder, List<T> items) {
    int count = 0;
    for (T item : items) {
      JobEntity child = new JobEntity();
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setStatus(JobStatus.PENDING);
      child.setPriority(JobPriority.NORMAL);
      child.setScheduledTime(Instant.now());
      child.setPayload(payload(builder.action(), List.of(item)));
      child.setIdempotencyKey(UUID.randomUUID().toString());
      child.setDependsOn(parentId);
      jobCrudStore.save(child);
      count++;
    }
    return count;
  }

  private void createWorkflowBranches(Long parentId, List<WorkflowBranch> branches) {
    for (WorkflowBranch branch : branches) {
      createWorkflowBranch(parentId, branch);
    }
  }

  private void createWorkflowBranch(Long parentId, WorkflowBranch branch) {
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

  private JobPayload payload(Serializable callback) {
    return validate(JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback)));
  }

  private JobPayload payload(Serializable callback, List<Object> runtimeArguments) {
    return validate(
        JobPayloadFactory.fromInvocation(
            jobInvocationResolver.resolve(callback, runtimeArguments)));
  }

  private JobPayload validate(JobPayload payload) {
    payloadValidator.validateAtCreation(payload);
    return payload;
  }
}
