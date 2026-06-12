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

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.event.JobSignalWaitingEvent;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.api.internal.JobBuilderState;
import run.ratchet.ri.core.internal.ChainScheduler;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/** CDI-managed persistence boundary for scheduler builders. */
@ApplicationScoped
class DefaultJobCreationService
    implements JobSubmitter, BatchSubmitter, StreamingBatchSubmitter, RecurringJobSubmitter {

  private static final Logger log = Logger.getLogger(DefaultJobCreationService.class);

  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobTerminalStore jobTerminalStore;
  private final JobCrudStore jobCrudStore;
  private final JobBulkStore jobBulkStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final RecurringJobStore recurringJobStore;
  private final JobWakeupService wakeupService;
  private final RecurringScheduler recurringScheduler;
  private final JobInvocationResolver jobInvocationResolver;
  private final JobPayloadInputValidator payloadValidator;
  private final CallerPrincipalProvider callerPrincipalProvider;
  private final TracingCollector tracingCollector;
  private final JobAuthorizationPolicy authorizationPolicy;
  private final ClassPolicy classPolicy;
  private final InternalEventPublisher eventPublisher;
  private final MetricsCollector metricsCollector;
  private final Clock clock;
  private final boolean signalCapabilityAvailable;
  private final boolean resourcePermitCapabilityAvailable;

  private volatile TransactionSynchronizationRegistry txRegistry;

  protected DefaultJobCreationService() {
    this.jobBatchStatusStore = null;
    this.jobTerminalStore = null;
    this.jobCrudStore = null;
    this.jobBulkStore = null;
    this.batchStore = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.recurringJobStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
    this.jobInvocationResolver = null;
    this.payloadValidator = null;
    this.callerPrincipalProvider = null;
    this.tracingCollector = null;
    this.authorizationPolicy = null;
    this.classPolicy = null;
    this.eventPublisher = null;
    this.metricsCollector = null;
    this.clock = null;
    this.signalCapabilityAvailable = false;
    this.resourcePermitCapabilityAvailable = false;
  }

  @Inject
  public DefaultJobCreationService(
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      Instance<BatchStore> batchStore,
      TagStore tagStore,
      Instance<WorkflowConditionStore> workflowConditionStore,
      Instance<RecurringJobStore> recurringJobStore,
      Instance<SignalStore> signalStore,
      Instance<ResourcePermitStore> resourcePermitStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      JobPayloadInputValidator payloadValidator,
      CallerPrincipalProvider callerPrincipalProvider,
      TracingCollector tracingCollector,
      JobAuthorizationPolicy authorizationPolicy,
      ClassPolicy classPolicy,
      InternalEventPublisher eventPublisher,
      MetricsCollector metricsCollector,
      Clock clock) {
    this(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        jobBulkStore,
        batchStore.isResolvable() ? batchStore.get() : null,
        tagStore,
        workflowConditionStore.isResolvable() ? workflowConditionStore.get() : null,
        recurringJobStore.isResolvable() ? recurringJobStore.get() : null,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        payloadValidator,
        callerPrincipalProvider,
        tracingCollector,
        authorizationPolicy,
        classPolicy,
        eventPublisher,
        metricsCollector,
        clock,
        signalStore.isResolvable(),
        resourcePermitStore.isResolvable());
  }

  /**
   * Constructor for tests that supply stores directly. Signal-waiting job creation is permitted
   * (assumes the {@code SignalStore} capability is present); pass through the {@code @Inject}
   * constructor to model an absent capability.
   */
  public DefaultJobCreationService(
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      JobPayloadInputValidator payloadValidator,
      CallerPrincipalProvider callerPrincipalProvider,
      TracingCollector tracingCollector,
      JobAuthorizationPolicy authorizationPolicy,
      ClassPolicy classPolicy,
      InternalEventPublisher eventPublisher,
      MetricsCollector metricsCollector,
      Clock clock) {
    this(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        jobBulkStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        payloadValidator,
        callerPrincipalProvider,
        tracingCollector,
        authorizationPolicy,
        classPolicy,
        eventPublisher,
        metricsCollector,
        clock,
        true,
        true);
  }

  private DefaultJobCreationService(
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      JobPayloadInputValidator payloadValidator,
      CallerPrincipalProvider callerPrincipalProvider,
      TracingCollector tracingCollector,
      JobAuthorizationPolicy authorizationPolicy,
      ClassPolicy classPolicy,
      InternalEventPublisher eventPublisher,
      MetricsCollector metricsCollector,
      Clock clock,
      boolean signalCapabilityAvailable,
      boolean resourcePermitCapabilityAvailable) {
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobTerminalStore = jobTerminalStore;
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.recurringJobStore = recurringJobStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
    this.jobInvocationResolver = jobInvocationResolver;
    this.payloadValidator = payloadValidator;
    this.callerPrincipalProvider = callerPrincipalProvider;
    this.tracingCollector = tracingCollector;
    this.authorizationPolicy = authorizationPolicy;
    this.classPolicy = classPolicy;
    this.eventPublisher = eventPublisher;
    this.metricsCollector = metricsCollector;
    this.clock = clock;
    this.signalCapabilityAvailable = signalCapabilityAvailable;
    this.resourcePermitCapabilityAvailable = resourcePermitCapabilityAvailable;
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the submission.
   */
  @Override
  @Transactional
  public JobHandle submit(JobBuilder builder) {
    JobBuilderState state = (JobBuilderState) builder;
    String idempotencyKey = state.idempotencyKey();
    Optional<JobEntity> existingByKey = jobCrudStore.findByIdempotencyKey(idempotencyKey);
    if (existingByKey.isPresent()) {
      UUID existingId = existingByKey.get().getId();
      log.debugf(
          "Duplicate idempotency key '%s', returning existing job %s", idempotencyKey, existingId);
      return () -> existingId;
    }

    String businessKey = state.businessKey();
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

    String signalKey = state.awaitSignalKey();
    boolean isSignalWaiting = signalKey != null;
    if (isSignalWaiting && !signalCapabilityAvailable) {
      // Refuse to persist a WAITING job that could never be delivered or timed out, since signal
      // delivery and the timeout scanner both require the SignalStore capability.
      throw new UnsupportedOperationException(
          "Signal-waiting job creation requires a store advertising the SignalStore capability");
    }
    String resourceName = builder.resourceName();
    boolean resourceGated = resourceName != null && !resourceName.isBlank();
    if (resourceGated && !resourcePermitCapabilityAvailable) {
      // The caller asked for concurrency gating on a named resource, but the store cannot enforce
      // it. Reject the submission rather than silently running the job with unbounded concurrency.
      throw new UnsupportedOperationException(
          "Job declares resource '"
              + resourceName
              + "' but the store does not advertise the ResourcePermitStore capability; resource"
              + " concurrency gating cannot be enforced");
    }
    Duration signalTimeout = isSignalWaiting ? state.awaitSignalTimeout() : null;
    Instant now = effective().instant();

    JobOptions opts = builder.opts();
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(isSignalWaiting ? JobStatus.WAITING : JobStatus.PENDING);
    job.setPriority(opts.priority());
    job.setScheduledTime(now.plus(state.delay()));
    job.setPayload(payload);
    if (isSignalWaiting) {
      job.setSignalKey(signalKey);
      job.setSignalTimeout(now.plus(signalTimeout));
    }
    job.setIdempotencyKey(idempotencyKey);
    job.setBusinessKey(businessKey);
    job.setResourceName(resourceName);
    job.setExecutionTarget(state.executionTarget());
    // Per-job opt-in from withEncryptedPayload(). The store write derives the actual
    // encrypted-or-not decision (global switch OR this flag) and the encryption_key_id column.
    job.setEncryptedPayload(state.encryptedPayload());
    if (builder.onSuccess() != null) {
      job.setOnSuccessPayload(payload(builder.onSuccess()));
    }
    if (state.onFailure() != null) {
      job.setOnFailurePayload(payload(state.onFailure()));
    }
    stampCallerPrincipal(job);
    captureTraceContext(job);
    applyOptions(job, opts);
    if (!builder.params().isEmpty()) {
      job.setParams(builder.params());
    }
    checkCreateAuthorization(job);

    JobEntity saved;
    try {
      saved = jobCrudStore.create(job);
    } catch (DuplicateIdempotencyKeyException e) {
      // A concurrent submission with the same idempotency key won the race to insert. Converge on
      // the documented idempotent result: re-resolve and return the existing job rather than
      // letting the constraint violation escape.
      JobEntity existing =
          jobCrudStore
              .findByIdempotencyKey(idempotencyKey)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Idempotency key '"
                              + idempotencyKey
                              + "' collided on insert but no job resolves by that key",
                          e));
      UUID existingId = existing.getId();
      log.debugf(
          "Idempotency key '%s' raced on insert, returning existing job %s",
          idempotencyKey, existingId);
      return () -> existingId;
    }
    UUID jobId = saved.getId();

    if (isSignalWaiting && eventPublisher != null) {
      publishSignalWaitingEvent(saved, signalKey, signalTimeout);
    }
    if (isSignalWaiting && metricsCollector != null) {
      metricsCollector.signalWaiting(jobId, saved.getPublicJobType(), signalKey);
    }

    List<String> tags = builder.tags();
    if (!tags.isEmpty()) {
      tagStore.insertTags(jobId, tags);
    }

    List<SerializableCheckedRunnable> chainTasks = state.chainTasks();
    if (!chainTasks.isEmpty()) {
      createChainSteps(jobId, chainTasks, opts, state.executionTarget(), state.encryptedPayload());
    }

    List<WorkflowBranch> branches = builder.workflowBranches();
    if (!branches.isEmpty()) {
      createWorkflowBranches(jobId, branches, state.executionTarget(), saved.isEncryptedPayload());
    }

    boolean shouldWakeup =
        builder.isImmediate() || opts.priority() == JobPriority.CRITICAL || state.delay().isZero();
    if (shouldWakeup) {
      wakeupService.notify(opts.priority(), true, state.executionTarget());
    }

    log.debugf("Job submitted (id=%s, type=SINGLE, delay=%s)", jobId, state.delay());
    return () -> jobId;
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the batch submission.
   */
  @Override
  @Transactional
  public JobHandle submit(DefaultBatchBuilder builder) {
    requireBatchCapability();
    JobEntity parent = newBatchParent();
    parent.setExecutionTarget(builder.executionTarget());
    checkCreateAuthorization(parent);
    JobEntity savedParent = jobCrudStore.create(parent);
    UUID parentId = savedParent.getId();

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

    List<JobEntity> childJobs = new ArrayList<>(builder.children().size());
    for (DefaultBatchBuilder.ChildSpec child : builder.children()) {
      JobEntity childJob = new JobEntity();
      childJob.setJobType(JobExecutionType.BATCH_CHILD);
      childJob.setStatus(JobStatus.PENDING);
      childJob.setPriority(JobPriority.NORMAL);
      childJob.setScheduledTime(effective().instant());
      childJob.setPayload(validate(child.payload()));
      childJob.setIdempotencyKey(UUID.randomUUID().toString());
      childJob.setDependsOn(parentId);
      childJob.setExecutionTarget(builder.executionTarget());
      stampCallerPrincipal(childJob);
      checkCreateAuthorization(childJob);
      childJobs.add(childJob);
    }
    bulkStore().bulkInsert(childJobs);

    for (WorkflowBranch branch : builder.workflowBranches()) {
      createWorkflowBranch(
          parentId, branch, builder.executionTarget(), savedParent.isEncryptedPayload());
    }

    wakeupService.notifyIfNeeded(
        JobExecutionType.BATCH_PARENT,
        JobPriority.NORMAL,
        Duration.ZERO,
        builder.executionTarget());

    log.infof(
        "Batch '%s' submitted with %s children (id=%s)",
        builder.name(), builder.children().size(), parentId);
    return () -> parentId;
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the streaming batch submission.
   */
  @Override
  @Transactional
  public <T extends Serializable> JobHandle submit(DefaultStreamingBatchBuilder<T> builder) {
    requireBatchCapability();
    builder.validateReady();

    JobEntity parent = newBatchParent();
    parent.setExecutionTarget(builder.executionTarget());
    checkCreateAuthorization(parent);
    JobEntity savedParent = jobCrudStore.create(parent);
    UUID parentId = savedParent.getId();

    int totalItems = 0;
    int chunksInserted = 0;
    List<T> chunk = builder.newChunk();
    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(0);
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (builder.batchProgressHook() != null) {
      batch.setProgressHook(payload(builder.batchProgressHook()));
    }
    batchStore.saveBatch(batch);

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
    batchStore.updateBatchTotalItems(parentId, totalItems);

    for (WorkflowBranch branch : builder.workflowBranches()) {
      createWorkflowBranch(
          parentId, branch, builder.executionTarget(), savedParent.isEncryptedPayload());
    }

    wakeupService.notifyIfNeeded(
        JobExecutionType.BATCH_PARENT,
        JobPriority.NORMAL,
        Duration.ZERO,
        builder.executionTarget());

    log.infof(
        "Streaming batch '%s' submitted with %s items (id=%s)",
        builder.name(), totalItems, parentId);
    return () -> parentId;
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the recurring submission.
   */
  @Override
  @Transactional
  public JobHandle submit(DefaultRecurringJobBuilder builder) {
    if (recurringJobStore == null) {
      throw new UnsupportedOperationException(
          "Recurring job submission requires a store advertising the RecurringJobStore capability");
    }
    Cron cron = RecurringScheduler.PARSER.parse(builder.cronExpr());
    cron.validate();

    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    Instant base = effective().instant();
    ZonedDateTime now = base.atZone(builder.zone());
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
    UUID id = UuidV7Factory.create();
    JobPayload validatedPayload = payload(builder.task());
    String callerPrincipal = resolveCallerPrincipal();
    // Authorization is type-aware: build a transient JobEntity for the policy gate (it inspects
    // job_type, priority, business key, target class). The transient entity is discarded — only
    // the new RecurringJobStore writes happen.
    JobEntity gate = new JobEntity();
    gate.setId(id);
    gate.setJobType(JobExecutionType.RECURRING);
    gate.setStatus(JobStatus.PENDING);
    gate.setPriority(options.priority());
    gate.setPayload(validatedPayload);
    gate.setBusinessKey(builder.businessKey());
    gate.setIdempotencyKey(UUID.randomUUID().toString());
    gate.setCronExpr(builder.cronExpr());
    gate.setZoneId(builder.zone().getId());
    gate.setNextFire(nextFire);
    gate.setExecutionTarget(builder.executionTarget());
    gate.setCallerPrincipal(callerPrincipal);
    applyOptions(gate, options);
    checkCreateAuthorization(gate);

    RecurringJobDefinition definition =
        new RecurringJobDefinition(
            id,
            builder.cronExpr(),
            builder.zone().getId(),
            nextFire,
            /* paused */ false,
            /* pausedAt */ null,
            options.priority().ordinal(),
            options.maxRetries(),
            options.backoffPolicy(),
            (int) options.backoffParam().toMillis(),
            options.timeoutSec(),
            validatedPayload,
            /* onSuccessPayload */ null,
            /* onFailurePayload */ null,
            builder.businessKey(),
            /* resourceName */ null,
            builder.executionTarget(),
            base,
            callerPrincipal,
            builder.encryptedPayload());

    UUID saved = recurringJobStore.createRecurring(definition);

    if (!builder.tags().isEmpty()) {
      tagStore.insertTags(saved, builder.tags());
    }

    log.infof(
        "Recurring job submitted (id=%s, cron=%s, zone=%s, nextFire=%s)",
        saved, builder.cronExpr(), builder.zone(), nextFire);

    recurringScheduler.kick();
    return () -> saved;
  }

  private void requireBatchCapability() {
    if (batchStore == null) {
      throw new UnsupportedOperationException(
          "Batch submission requires a store advertising the BatchStore capability");
    }
  }

  private String resolveCallerPrincipal() {
    return callerPrincipalProvider == null
        ? null
        : callerPrincipalProvider.currentPrincipal().orElse(null);
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
    parent.setScheduledTime(effective().instant());
    parent.setPayload(validate(JobPayloadFactory.noop()));
    parent.setIdempotencyKey(UUID.randomUUID().toString());
    stampCallerPrincipal(parent);
    return parent;
  }

  private void completeEmptyBatch(UUID parentId) {
    if (jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
      Instant now = effective().instant();
      jobTerminalStore.markJobSucceededMinimal(parentId, now, now, 0L, 0L);
      batchStore.markBatchCompleteIfReady(parentId);
    }
  }

  private void createChainSteps(
      UUID predecessorId,
      List<SerializableCheckedRunnable> chainTasks,
      JobOptions opts,
      String executionTarget,
      boolean parentEncrypted) {
    UUID prevId = predecessorId;
    for (SerializableCheckedRunnable chainTask : chainTasks) {
      JobEntity step = new JobEntity();
      step.setJobType(JobExecutionType.CHAIN_STEP);
      step.setStatus(JobStatus.PENDING);
      step.setPriority(opts.priority());
      step.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
      step.setPayload(payload(chainTask));
      // A chain step inherits its parent's encryption opt-in: the row mapper encrypts the step's
      // own
      // payload args off this flag, exactly as it does for the parent job, so an opted-in
      // .then(...)
      // chain never persists plaintext args.
      step.setEncryptedPayload(parentEncrypted);
      step.setIdempotencyKey(UUID.randomUUID().toString());
      step.setDependsOn(prevId);
      step.setExecutionTarget(executionTarget);
      applyOptions(step, opts);
      stampCallerPrincipal(step);
      captureTraceContext(step);
      checkCreateAuthorization(step);

      JobEntity savedStep = jobCrudStore.create(step);
      prevId = savedStep.getId();
    }
  }

  private <T extends Serializable> int createStreamingChildJobs(
      UUID parentId, DefaultStreamingBatchBuilder<T> builder, List<T> items) {
    List<JobEntity> children = new ArrayList<>(items.size());
    for (T item : items) {
      JobEntity child = new JobEntity();
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setStatus(JobStatus.PENDING);
      child.setPriority(JobPriority.NORMAL);
      child.setScheduledTime(effective().instant());
      child.setPayload(payload(builder.action(), List.of(item)));
      child.setIdempotencyKey(UUID.randomUUID().toString());
      child.setDependsOn(parentId);
      child.setExecutionTarget(builder.executionTarget());
      stampCallerPrincipal(child);
      checkCreateAuthorization(child);
      children.add(child);
    }
    bulkStore().bulkInsert(children);
    return children.size();
  }

  private JobBulkStore bulkStore() {
    if (jobBulkStore == null) {
      throw new IllegalStateException(
          "Batch submission requires a JobBulkStore; use the CDI constructor or pass a store that"
              + " implements JobBulkStore.");
    }
    return jobBulkStore;
  }

  private void createWorkflowBranches(
      UUID parentId,
      List<WorkflowBranch> branches,
      String executionTarget,
      boolean parentEncrypted) {
    if (workflowConditionStore == null && !branches.isEmpty()) {
      throw new UnsupportedOperationException(
          "Workflow branch scheduling requires a store advertising the WorkflowConditionStore"
              + " capability");
    }
    for (WorkflowBranch branch : branches) {
      createWorkflowBranch(parentId, branch, executionTarget, parentEncrypted);
    }
  }

  private void createWorkflowBranch(
      UUID parentId, WorkflowBranch branch, String executionTarget, boolean parentEncrypted) {
    JobEntity branchJob = new JobEntity();
    branchJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    branchJob.setStatus(JobStatus.PENDING);
    branchJob.setPriority(JobPriority.NORMAL);
    branchJob.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    branchJob.setPayload(payload(branch.task()));
    branchJob.setEncryptedPayload(parentEncrypted);
    branchJob.setIdempotencyKey(UUID.randomUUID().toString());
    branchJob.setDependsOn(parentId);
    branchJob.setExecutionTarget(executionTarget);
    stampCallerPrincipal(branchJob);
    checkCreateAuthorization(branchJob);
    JobEntity savedBranch = jobCrudStore.create(branchJob);

    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setParentJobId(parentId);
    condition.setChildJobId(savedBranch.getId());
    condition.setConditionType(branch.condition().type());
    condition.setConditionPriority(branch.condition().priority());
    if (branch.condition().expression() != null) {
      Serializable expr = branch.condition().expression();
      if (expr instanceof SerializablePredicate<?>
          || expr instanceof SerializableFunction<?, ?>
          || expr instanceof JobInvocation) {
        JobPayload p =
            expr instanceof JobInvocation invocation
                ? JobPayloadFactory.fromInvocation(invocation)
                : JobPayloadFactory.fromLambda(expr);
        // The predicate belongs to the parent job, binds the parent id, and follows the parent's
        // encryption opt-in: an opted-in workflow encrypts its predicate even when the global
        // switch is off.
        condition.setConditionExpression(
            PayloadEncryptor.encryptArgs(
                PayloadSerializerHolder.get().serialize(p),
                EncryptionHolder.encryptionActiveFor(parentEncrypted),
                EncryptionTarget.predicate(parentId)));
      } else {
        condition.setConditionExpression(expr.toString());
      }
    }
    workflowConditionStore.saveCondition(condition);
  }

  private JobPayload payload(Serializable callback) {
    // Pre-resolved invocations (from InvocationSubmissionService facades) skip lambda resolution;
    // they still run through the same validation and class-policy gate below.
    if (callback instanceof InvocationAdapter adapter) {
      return validate(JobPayloadFactory.fromInvocation(adapter.invocation()));
    }
    if (callback instanceof JobInvocation invocation) {
      return validate(JobPayloadFactory.fromInvocation(invocation));
    }
    return validate(JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback)));
  }

  private JobPayload payload(Serializable callback, List<Object> runtimeArguments) {
    return validate(
        JobPayloadFactory.fromInvocation(
            jobInvocationResolver.resolve(callback, runtimeArguments)));
  }

  private JobPayload validate(JobPayload payload) {
    payloadValidator.validateAtCreation(payload);
    // Persist-time ClassPolicy gate: mirrors WorkflowConditionEvaluator and JobSecurityValidator
    // at execution time. Closes the TOCTOU window between persistence and execution — a malicious
    // lambda's target class is rejected before reaching the database, not after a worker dequeues
    // it. Framework coordination payloads (the batch-parent noop) target an internal placeholder,
    // never run user code, and so are not subject to the application allowlist — gating them would
    // break batch submission for any app that allowlists only its own packages.
    if (classPolicy != null
        && payload != null
        && payload.target() != null
        && !JobPayloadFactory.isCoordinationPlaceholder(payload)
        && !classPolicy.isAllowed(payload.target())) {
      throw new SecurityException(
          "Class " + payload.target() + " is not allowed for job execution.");
    }
    return payload;
  }

  private void stampCallerPrincipal(JobEntity job) {
    if (callerPrincipalProvider == null) {
      return;
    }
    callerPrincipalProvider.currentPrincipal().ifPresent(job::setCallerPrincipal);
  }

  private void checkCreateAuthorization(JobEntity job) {
    if (authorizationPolicy == null) {
      return;
    }
    // Pre-assign a UUIDv7 so the policy receives a stable identifier. @PrePersist would
    // normally assign it during save(); since checkCreate fires before save, we assign here.
    // UuidV7EntityListener's null-check ensures the pre-assigned value is not overwritten.
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
    }
    authorizationPolicy.checkCreate(job.getId(), job.getCallerPrincipal());
  }

  private void publishSignalWaitingEvent(
      JobEntity saved, String signalKey, Duration signalTimeout) {
    JobSignalWaitingEvent event =
        new JobSignalWaitingEvent(
            saved.getId(),
            saved.getBusinessKey(),
            saved.getPublicJobType(),
            saved.getPriority(),
            null,
            signalKey,
            signalTimeout);
    if (!registerAfterCommit(() -> eventPublisher.publish(event))) {
      eventPublisher.publish(event);
    }
  }

  private boolean registerAfterCommit(Runnable action) {
    return JobWakeupService.registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit signal waiting event registration failed; publishing immediately: %s");
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry reg = txRegistry;
    if (reg == null) {
      synchronized (this) {
        reg = txRegistry;
        if (reg == null) {
          reg = JobWakeupService.lookupTxRegistry(log);
          txRegistry = reg;
        }
      }
    }
    return reg;
  }

  void setTxRegistryForTesting(TransactionSynchronizationRegistry txRegistry) {
    this.txRegistry = txRegistry;
  }

  /**
   * Captures the active trace context from the submitting thread and stores it on the job entity.
   *
   * <p>The stored map is passed back to {@link TracingCollector#jobExecutionStarted} at execution
   * time so the executing span can be parented to the original caller's trace.
   *
   * <p>Called for single-job roots and chain steps (both created on the submitting thread, so the
   * caller's span is still active). Batch children and recurring jobs are excluded: batch children
   * are created in tight loops where per-item context capture adds overhead without proportional
   * signal, and recurring jobs fire on a future cron schedule where the scheduling-time span is
   * long-closed by execution time.
   */
  private void captureTraceContext(JobEntity job) {
    if (tracingCollector == null) {
      return;
    }
    Map<String, String> ctx = tracingCollector.captureCurrentContext();
    if (!ctx.isEmpty()) {
      job.setTraceContext(ctx);
    }
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
