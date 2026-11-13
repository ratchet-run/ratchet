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
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jboss.logging.Logger;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.JobType;
import run.ratchet.api.KeyRange;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition.ConditionType;
import run.ratchet.api.event.BatchChunkFailureEvent;
import run.ratchet.api.event.JobSignalWaitingEvent;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.api.internal.JobBuilderState;
import run.ratchet.ri.core.internal.AfterCommitActions;
import run.ratchet.ri.core.internal.ChainScheduler;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.RecurringFireTime;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.CallerPrincipalResolution;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.CallerPrincipalResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/** CDI-managed persistence boundary for scheduler builders. */
@ApplicationScoped
public class DefaultJobCreationService
    implements JobSubmitter, BatchSubmitter, StreamingBatchSubmitter, RecurringJobSubmitter {

  private PartitionService partitionService;

  /** Connects partition generation support. */
  @Inject
  public void configurePartitions(PartitionService service) {
    this.partitionService = service;
  }

  private static final Logger log = Logger.getLogger(DefaultJobCreationService.class);
  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();

  private final JobCrudStore jobCrudStore;
  private final JobBulkStore jobBulkStore;
  private final BatchStore batchStore;
  private final BatchService batchService;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final RecurringJobStore recurringJobStore;
  private final JobWakeupService wakeupService;
  private final RecurringScheduler recurringScheduler;
  private final JobInvocationResolver jobInvocationResolver;
  private final JobPayloadInputValidator payloadValidator;
  private final CallerPrincipalProvider callerPrincipalProvider;
  private final CallerPrincipalResolver callerPrincipalResolver;
  private final TracingCollector tracingCollector;
  private final JobAuthorizationPolicy authorizationPolicy;
  private final ClassPolicy classPolicy;
  private final JobPayloadInvoker payloadInvoker;
  private boolean transactionalCompletionAvailable;
  private final InternalEventPublisher eventPublisher;
  private final ErrorSanitizer errorSanitizer;
  private final MetricsCollector metricsCollector;
  private final Clock clock;
  private final boolean signalCapabilityAvailable;
  private final boolean resourcePermitCapabilityAvailable;

  private final AfterCommitRegistrar afterCommitRegistrar;
  private final BatchCompletionTransaction conflictResolutionTransaction;

  protected DefaultJobCreationService() {
    this.jobCrudStore = null;
    this.jobBulkStore = null;
    this.batchStore = null;
    this.batchService = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.recurringJobStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
    this.jobInvocationResolver = null;
    this.payloadValidator = null;
    this.callerPrincipalProvider = null;
    this.callerPrincipalResolver = null;
    this.tracingCollector = null;
    this.authorizationPolicy = null;
    this.classPolicy = null;
    this.payloadInvoker = null;
    this.eventPublisher = null;
    this.errorSanitizer = null;
    this.metricsCollector = null;
    this.clock = null;
    this.afterCommitRegistrar = null;
    this.conflictResolutionTransaction = null;
    this.signalCapabilityAvailable = false;
    this.resourcePermitCapabilityAvailable = false;
  }

  @Inject
  public DefaultJobCreationService(
      BatchService batchService,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      Instance<BatchStore> batchStore,
      TagStore tagStore,
      Instance<WorkflowConditionStore> workflowConditionStore,
      Instance<RecurringJobStore> recurringJobStore,
      Instance<SignalStore> signalStore,
      Instance<ResourcePermitStore> resourcePermitStore,
      Instance<TransactionalCompletionStore> transactionalCompletionStore,
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
      RatchetOptions options,
      AfterCommitRegistrar afterCommitRegistrar,
      ErrorSanitizer errorSanitizer,
      BatchCompletionTransaction conflictResolutionTransaction) {
    this(
        batchService,
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
        resourcePermitStore.isResolvable(),
        options != null ? options.callerPrincipalResolver() : null,
        afterCommitRegistrar,
        errorSanitizer,
        conflictResolutionTransaction);
    this.transactionalCompletionAvailable = transactionalCompletionStore.isResolvable();
  }

  public DefaultJobCreationService(
      BatchService batchService,
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
      boolean resourcePermitCapabilityAvailable,
      CallerPrincipalResolver callerPrincipalResolver,
      AfterCommitRegistrar afterCommitRegistrar,
      ErrorSanitizer errorSanitizer) {
    this(
        batchService,
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
        signalCapabilityAvailable,
        resourcePermitCapabilityAvailable,
        callerPrincipalResolver,
        afterCommitRegistrar,
        errorSanitizer,
        null);
  }

  /** Creates the service with the container-managed helper used for conflict-resolution reads. */
  public DefaultJobCreationService(
      BatchService batchService,
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
      boolean resourcePermitCapabilityAvailable,
      CallerPrincipalResolver callerPrincipalResolver,
      AfterCommitRegistrar afterCommitRegistrar,
      ErrorSanitizer errorSanitizer,
      BatchCompletionTransaction conflictResolutionTransaction) {
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.batchStore = batchStore;
    this.batchService = batchService;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.recurringJobStore = recurringJobStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
    this.jobInvocationResolver = jobInvocationResolver;
    this.payloadValidator = payloadValidator;
    this.callerPrincipalProvider = callerPrincipalProvider;
    this.callerPrincipalResolver = callerPrincipalResolver;
    this.tracingCollector = tracingCollector;
    this.authorizationPolicy = authorizationPolicy;
    this.classPolicy = classPolicy;
    this.payloadInvoker = new JobPayloadInvoker(null, classPolicy);
    this.transactionalCompletionAvailable =
        jobCrudStore instanceof JobStore store
            && store.capability(TransactionalCompletionStore.class).isPresent();
    this.eventPublisher = eventPublisher;
    this.errorSanitizer = errorSanitizer;
    this.metricsCollector = metricsCollector;
    this.clock = clock;
    this.afterCommitRegistrar = afterCommitRegistrar;
    this.conflictResolutionTransaction = conflictResolutionTransaction;
    this.signalCapabilityAvailable = signalCapabilityAvailable;
    this.resourcePermitCapabilityAvailable = resourcePermitCapabilityAvailable;
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the submission. A duplicate business key does not mark the
   *     caller's transaction rollback-only.
   */
  @Override
  @Transactional(dontRollbackOn = DuplicateBusinessKeyException.class)
  public JobHandle submit(JobBuilder builder) {
    JOB_PAYLOAD_CONVERTER.beginPreparationScope();
    try {
      return submitPrepared(builder);
    } finally {
      JOB_PAYLOAD_CONVERTER.endPreparationScope();
    }
  }

  private JobHandle submitPrepared(JobBuilder builder) {
    JobBuilderState state = (JobBuilderState) builder;
    String idempotencyKey = state.idempotencyKey();
    Optional<UUID> existingByKey = jobCrudStore.findOriginalJobIdByIdempotencyKey(idempotencyKey);
    if (existingByKey.isPresent()) {
      UUID existingId = existingByKey.get();
      log.debugf(
          "Duplicate idempotency key '%s', returning existing job %s", idempotencyKey, existingId);
      return () -> existingId;
    }

    String businessKey = state.businessKey();
    if (businessKey != null) {
      Optional<JobEntity> activeByBk = jobCrudStore.findActiveByBusinessKey(businessKey);
      if (activeByBk.isPresent()) {
        // A racer with the same idempotency key can commit between the two lookups. Its job is
        // the one the idempotency lookup would have returned, so resolve to it the same way.
        return resolveActiveBusinessKey(activeByBk.get(), businessKey, idempotencyKey);
      }
    }

    JobPayload payload = payload(builder.task());
    List<JobPayload> chainPayloads = state.chainTasks().stream().map(this::payload).toList();
    List<BranchPayloads> branches = prepareWorkflowBranches(builder.workflowBranches(), null);

    String signalKey = state.awaitSignalKey();
    boolean isSignalWaiting = signalKey != null;
    if (isSignalWaiting && !signalCapabilityAvailable) {
      // Refuse to persist a WAITING job that could never be delivered or timed out, since signal
      // delivery and the timeout scanner both require the SignalStore capability.
      throw new UnsupportedOperationException(
          "Signal-waiting job creation requires a store advertising the SignalStore capability");
    }
    String resourceName = builder.resourceName();
    requireResourcePermitCapability(resourceName, "Job declares resource");
    Duration signalTimeout = isSignalWaiting ? state.awaitSignalTimeout() : null;
    Instant now = effective().instant();

    JobOptions opts = builder.opts();
    String callerPrincipal = resolveCallerPrincipal();
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
      job.setOnSuccessPayload(runtimePayload(builder.onSuccess()));
    }
    if (state.onFailure() != null) {
      job.setOnFailurePayload(runtimePayload(state.onFailure()));
    }
    stampCallerPrincipal(job, callerPrincipal);
    captureTraceContext(job);
    applyOptions(job, opts);
    if (!builder.params().isEmpty()) {
      job.setParams(builder.params());
    }
    checkCreateAuthorization(job);

    JobEntity saved;
    try {
      saved = createJob(job);
    } catch (BusinessKeyReservationConflictException e) {
      if (state.hasExplicitIdempotencyKey()) {
        Optional<UUID> original;
        try {
          // A fresh transaction can see the winner outside a MySQL REPEATABLE READ snapshot.
          original =
              conflictResolutionTransaction != null
                  ? conflictResolutionTransaction.complete(
                      () -> jobCrudStore.findOriginalJobIdByIdempotencyKey(idempotencyKey))
                  : jobCrudStore.findOriginalJobIdByIdempotencyKey(idempotencyKey);
        } catch (RuntimeException lookupFailure) {
          e.addSuppressed(lookupFailure);
          throw e;
        }
        if (original.isPresent()) {
          UUID originalId = original.get();
          return () -> originalId;
        }
      }
      throw new DuplicateBusinessKeyException(
          businessKey, "Active job already exists with business key '" + businessKey + "'", e);
    }
    UUID jobId = saved.getId();

    List<String> tags = builder.tags();
    if (!tags.isEmpty()) {
      tagStore.insertTags(jobId, tags);
    }

    if (!chainPayloads.isEmpty()) {
      createChainSteps(
          jobId,
          chainPayloads,
          opts,
          state.executionTarget(),
          state.encryptedPayload(),
          callerPrincipal);
    }

    if (!branches.isEmpty()) {
      createWorkflowBranches(
          jobId,
          branches,
          opts,
          state.executionTarget(),
          saved.isEncryptedPayload(),
          callerPrincipal);
    }

    // Publish after the last write: a failed registration throws, and a store whose writes do not
    // join the transaction (MongoDB) would keep a job without its tags, steps or branches.
    if (isSignalWaiting && eventPublisher != null) {
      publishSignalWaitingEvent(saved, signalKey, signalTimeout);
    }
    if (isSignalWaiting && metricsCollector != null) {
      metricsCollector.signalWaiting(jobId, saved.getPublicJobType(), signalKey);
    }

    boolean shouldWakeup =
        builder.isImmediate() || opts.priority() == JobPriority.CRITICAL || state.delay().isZero();
    if (shouldWakeup) {
      wakeupService.notify(opts.priority(), true, state.executionTarget());
    }

    log.debugf("Job submitted (id=%s, type=SINGLE, delay=%s)", jobId, state.delay());
    return () -> jobId;
  }

  private JobHandle resolveActiveBusinessKey(
      JobEntity active, String businessKey, String idempotencyKey) {
    UUID activeId = active.getId();
    if (idempotencyKey.equals(active.getIdempotencyKey())) {
      log.debugf(
          "Duplicate idempotency key '%s', returning existing job %s", idempotencyKey, activeId);
      return () -> activeId;
    }
    throw new DuplicateBusinessKeyException(
        businessKey,
        "Active job already exists with business key '"
            + businessKey
            + "' (jobId="
            + activeId
            + ")");
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the batch submission.
   */
  @Override
  @Transactional
  public JobHandle submit(DefaultBatchBuilder builder) {
    JOB_PAYLOAD_CONVERTER.beginPreparationScope();
    try {
      return submitPrepared(builder);
    } finally {
      JOB_PAYLOAD_CONVERTER.endPreparationScope();
    }
  }

  private JobHandle submitPrepared(DefaultBatchBuilder builder) {
    String idempotencyKey = builder.idempotencyKey();
    if (idempotencyKey != null) {
      Optional<UUID> existingByKey = jobCrudStore.findOriginalJobIdByIdempotencyKey(idempotencyKey);
      if (existingByKey.isPresent()) {
        UUID existingId = existingByKey.get();
        log.debugf(
            "Duplicate idempotency key '%s', returning existing job %s",
            idempotencyKey, existingId);
        return () -> existingId;
      }
    }
    requireBatchCapability();
    requireResourcePermitCapability(builder.childResourceName(), "Batch declares child resource");
    if (builder.partitionKey() != null) return submitPartitions(builder);
    if (builder.reducer() != null) {
      builder.reducer().validate(classPolicy, PayloadSerializerHolder.get());
      validateReducingChildren(builder);
    }
    List<JobPayload> childPayloads =
        builder.children().stream()
            .map(child -> validateRunnable(JobPayloadFactory.requireTaskArguments(child.payload())))
            .toList();
    JobPayload progressPayload =
        builder.progressHook() == null ? null : runtimePayload(builder.progressHook());
    List<BranchPayloads> branches =
        prepareWorkflowBranches(
            builder.workflowBranches(),
            builder.reducer() == null ? null : builder.reducer().successBranch());
    String callerPrincipal = resolveCallerPrincipal();
    JobEntity parent = newBatchParent(callerPrincipal, idempotencyKey);
    parent.setExecutionTarget(builder.executionTarget());
    checkCreateAuthorization(parent);
    JobEntity savedParent = createJob(parent);
    UUID parentId = savedParent.getId();

    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    if (builder.reducer() != null) {
      batch.setReducer(
          PayloadEncryptor.encryptJsonColumn(
              builder.reducer().json(PayloadSerializerHolder.get()),
              EncryptionHolder.encryptionActiveFor(savedParent.isEncryptedPayload()),
              EncryptionTarget.rowBound(ProtectedSurface.BATCH_REDUCER, parentId)));
    }
    batch.setTotalItems(builder.children().size());
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (builder.progressHook() != null) {
      batch.setProgressHook(progressPayload);
    }
    saveBatch(batch);

    List<JobEntity> childJobs = new ArrayList<>(builder.children().size());
    for (JobPayload childPayload : childPayloads) {
      JobEntity childJob = new JobEntity();
      childJob.setJobType(JobExecutionType.BATCH_CHILD);
      childJob.setStatus(JobStatus.PENDING);
      childJob.setPriority(JobPriority.NORMAL);
      childJob.setScheduledTime(effective().instant());
      childJob.setPayload(childPayload);
      childJob.setIdempotencyKey(UUID.randomUUID().toString());
      childJob.setDependsOn(parentId);
      childJob.setExecutionTarget(builder.executionTarget());
      childJob.setResourceName(builder.childResourceName());
      applyOptions(childJob, builder.childOptions());
      stampCallerPrincipal(childJob, callerPrincipal);
      checkCreateAuthorization(childJob);
      childJobs.add(childJob);
    }
    createWorkflowBranches(
        parentId,
        branches,
        builder.callbackOptions(),
        builder.executionTarget(),
        savedParent.isEncryptedPayload(),
        callerPrincipal);

    if (!childJobs.isEmpty()) {
      bulkInsertJobs(childJobs);
    }

    if (childJobs.isEmpty()) {
      if (batchService.completeEmptyBatch(parentId, batch)) {
        wakeupService.notify(JobPriority.NORMAL, true, builder.executionTarget());
      }
      log.infof(
          "Batch '%s' submitted with 0 children — completed immediately (id=%s)",
          builder.name(), parentId);
      return () -> parentId;
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

  @Override
  public int maxPartitions() {
    return partitionService == null ? Integer.MAX_VALUE : partitionService.maxPartitions();
  }

  private JobHandle submitPartitions(DefaultBatchBuilder builder) {
    PartitionStore partitions = partitionService == null ? null : partitionService.store();
    if (partitions == null)
      throw new UnsupportedOperationException(
          "Partitioned batches require a store advertising the PartitionStore capability");
    if (builder.reducer() != null)
      throw new IllegalStateException("Partitions cannot reduce results");
    if (!builder.reopened().isEmpty() && builder.idempotencyKey() == null)
      throw new IllegalStateException("Reopening partitions requires an idempotency key");
    if (builder.partitions().size() > partitionService.maxPartitions())
      throw new IllegalArgumentException("Too many partitions");
    if (builder.name() == null || builder.name().isBlank() || builder.name().length() > 255)
      throw new IllegalArgumentException("Partition set name must contain 1 to 255 characters");
    requireWorkflowConditionCapability(builder.workflowBranches());
    String caller = resolveCallerPrincipal();
    Instant now = effective().instant();
    UUID parentId = UuidV7Factory.create();
    PartitionSetEntity set = partitions.findPartitionSet(builder.partitionKey()).orElse(null);
    Integer expectedVersion = null;
    List<PartitionEntity> rows = new ArrayList<>();
    if (set == null) {
      if (!builder.reopened().isEmpty())
        throw new IllegalArgumentException("Cannot reopen a new partition set");
      set = new PartitionSetEntity();
      set.setSetId(UuidV7Factory.create());
      set.setSetKey(builder.partitionKey());
      set.setName(builder.name());
      set.setPartitionCount(builder.partitions().size());
      set.setLatestGeneration(1);
      set.setSealedGeneration(0);
      set.setCreatedAt(now);
      set.setOwnerPrincipal(caller);
      for (int index = 0; index < builder.partitions().size(); index++) {
        KeyRange range = builder.partitions().get(index);
        PartitionEntity row = new PartitionEntity();
        row.setSetId(set.getSetId());
        row.setPartitionIndex(index);
        row.setRangeFrom(range.from());
        row.setRangeTo(range.to());
        rows.add(row);
      }
    } else {
      if (authorizationPolicy != null)
        authorizationPolicy.checkRetry(set.getLatestBatchId(), set.getOwnerPrincipal(), caller);
      List<PartitionEntity> stored = new ArrayList<>();
      int after = -1;
      while (true) {
        List<PartitionEntity> page = partitions.findPartitions(set.getSetId(), after, 500);
        if (page.isEmpty()) break;
        stored.addAll(page);
        after = page.get(page.size() - 1).getPartitionIndex();
      }
      if (stored.size() != builder.partitions().size())
        throw new IllegalArgumentException("Partition ranges changed");
      for (int index = 0; index < stored.size(); index++) {
        var row = stored.get(index);
        var range = builder.partitions().get(index);
        if (row.getPartitionIndex() != index
            || row.getRangeFrom() != range.from()
            || row.getRangeTo() != range.to())
          throw new IllegalArgumentException("Partition ranges changed");
      }
      if (set.getSealedGeneration() < set.getLatestGeneration()) {
        partitionService.trySeal(set);
        set = partitions.findPartitionSet(builder.partitionKey()).orElseThrow();
        if (set.getSealedGeneration() < set.getLatestGeneration()) {
          if (!builder.reopened().isEmpty())
            throw new IllegalStateException(
                "the previous generation of partition set '"
                    + builder.partitionKey()
                    + "' is still running");
          UUID latest = set.getLatestBatchId();
          return () -> latest;
        }
        // Lazy sealing updated outcomes and checkpoints; reload before choosing targets.
        stored.clear();
        after = -1;
        while (true) {
          var page = partitions.findPartitions(set.getSetId(), after, 500);
          if (page.isEmpty()) break;
          stored.addAll(page);
          after = page.get(page.size() - 1).getPartitionIndex();
        }
      }
      for (var row : stored) {
        boolean reopen = builder.reopened().contains(row.getPartitionIndex());
        if (row.getOutcome() != PartitionOutcome.SUCCEEDED || reopen) {
          if (reopen) row.setCheckpoint(null);
          rows.add(row);
        }
      }
      if (rows.isEmpty()) {
        UUID latest = set.getLatestBatchId();
        return () -> latest;
      }
      expectedVersion = set.getVersion();
      set.setLatestGeneration(set.getLatestGeneration() + 1);
    }
    set.setLatestBatchId(parentId);
    set.setGenerationStartedAt(now);
    set.setUpdatedAt(now);
    for (var row : rows) {
      row.setGeneration(set.getLatestGeneration());
      row.setChildJobId(UuidV7Factory.create());
      row.setOutcome(null);
      row.setLastError(null);
    }
    JobEntity parent = newBatchParent(caller, builder.idempotencyKey());
    parent.setId(parentId);
    parent.setExecutionTarget(builder.executionTarget());
    checkCreateAuthorization(parent);
    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setPartitionSetId(set.getSetId());
    batch.setGeneration(set.getLatestGeneration());
    batch.setTotalItems(rows.size());
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (builder.progressHook() != null)
      batch.setProgressHook(runtimePayload(builder.progressHook()));
    requireBulkStore();
    List<JobEntity> children = new ArrayList<>();
    for (var row : rows) {
      JobEntity child = new JobEntity();
      child.setId(row.getChildJobId());
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setStatus(JobStatus.PENDING);
      child.setPriority(JobPriority.NORMAL);
      child.setScheduledTime(now);
      child.setPayload(
          validateRunnable(
              builder.partitionPayload(new KeyRange(row.getRangeFrom(), row.getRangeTo()))));
      child.setIdempotencyKey(UUID.randomUUID().toString());
      child.setDependsOn(parentId);
      child.setExecutionTarget(builder.executionTarget());
      child.setResourceName(builder.childResourceName());
      applyOptions(child, builder.childOptions());
      stampCallerPrincipal(child, caller);
      checkCreateAuthorization(child);
      children.add(child);
    }
    List<BranchPayloads> branchPayloads = prepareWorkflowBranches(builder.workflowBranches(), null);
    List<PreparedWorkflowBranch> branches = new ArrayList<>();
    for (int order = 0; order < branchPayloads.size(); order++) {
      branches.add(
          prepareWorkflowBranch(
              parentId,
              branchPayloads.get(order),
              order,
              builder.callbackOptions(),
              builder.executionTarget(),
              parent.isEncryptedPayload(),
              caller));
    }
    if (!partitions.createOrAdvance(
        new PartitionGenerationWrite(set, expectedVersion, rows, parent, batch)))
      throw new IllegalStateException(
          "Partition set '" + builder.partitionKey() + "' was submitted concurrently; retry");
    try {
      for (PreparedWorkflowBranch branch : branches) {
        persistWorkflowBranch(branch);
      }
      bulkInsertJobs(children);
    } catch (RuntimeException | Error failure) {
      try {
        partitionService.cancelGeneration(parentId);
      } catch (RuntimeException | Error cancellationFailure) {
        if (cancellationFailure != failure) {
          failure.addSuppressed(cancellationFailure);
        }
      }
      throw failure;
    }
    // Once all children exist, the sweep's count < totalItems check cannot fire.
    // Every callback was written before the children, so cancellation finds them all.
    // A failure from here on leaves the complete generation running. If the parent was
    // canceled, the sweep runs the same cleanup before it seals the generation.
    if (jobCrudStore.getJobStatus(parentId) == JobStatus.CANCELED) {
      batchService.cancelBatch(parent);
      log.warnf("Canceled late partition writes for parent %s in set %s", parentId, set.getSetId());
    }
    wakeupService.notifyIfNeeded(
        JobExecutionType.BATCH_PARENT,
        JobPriority.NORMAL,
        Duration.ZERO,
        builder.executionTarget());
    return () -> parentId;
  }

  /**
   * @implSpec Transaction attribute: REQUIRED. A caller transaction is joined; otherwise the
   *     container starts one for the streaming batch submission.
   */
  @Override
  @Transactional
  // The internal builder crosses a public interface method that must retain transaction advice.
  @SuppressWarnings("ClassEscapesDefinedScope")
  public <T extends Serializable> JobHandle submit(DefaultStreamingBatchBuilder<T> builder) {
    JOB_PAYLOAD_CONVERTER.beginPreparationScope();
    try {
      return submitPrepared(builder);
    } finally {
      JOB_PAYLOAD_CONVERTER.endPreparationScope();
    }
  }

  private <T extends Serializable> JobHandle submitPrepared(
      DefaultStreamingBatchBuilder<T> builder) {
    // start() owns the caller's stream and closes it on every path, including a failed check, a
    // duplicate key and a failed insert. A null stream (fromStream() never called) is skipped. If
    // the submit failed, a close() failure is added to it as suppressed; after a normal return, a
    // close() failure propagates, as with any try-with-resources.
    try (Stream<T> stream = builder.stream()) {
      requireBatchCapability();
      requireResourcePermitCapability(builder.childResourceName(), "Batch declares child resource");
      builder.validateReady();
      return submitStream(builder);
    }
  }

  private <T extends Serializable> JobHandle submitStream(DefaultStreamingBatchBuilder<T> builder) {
    String idempotencyKey = builder.idempotencyKey();
    if (idempotencyKey != null) {
      Optional<UUID> existingByKey = jobCrudStore.findOriginalJobIdByIdempotencyKey(idempotencyKey);
      if (existingByKey.isPresent()) {
        UUID existingId = existingByKey.get();
        log.debugf(
            "Duplicate idempotency key '%s', returning existing job %s",
            idempotencyKey, existingId);
        return () -> existingId;
      }
    }

    List<JobPayload> stagedChildren = null;
    if (builder.reducer() != null) {
      builder.reducer().validate(classPolicy, PayloadSerializerHolder.get());
      validateReducingChildren(builder);
      stagedChildren = prepareReducingInvocationStream(builder);
    }
    var stagedIterator = stagedChildren == null ? null : stagedChildren.iterator();
    Function<T, JobPayload> childPayloadFactory =
        stagedIterator == null
            ? item -> streamingPayload(builder, item)
            : item -> stagedIterator.next();
    List<BranchPayloads> branches =
        prepareWorkflowBranches(
            builder.workflowBranches(),
            builder.reducer() == null ? null : builder.reducer().successBranch());
    JobPayload progressPayload =
        builder.batchProgressHook() == null ? null : runtimePayload(builder.batchProgressHook());
    var iterator = builder.stream().iterator();
    List<T> firstItems = builder.newChunk();
    JobPayload firstPayload = null;
    if (iterator.hasNext()) {
      T first = iterator.next();
      firstItems.add(first);
      firstPayload = childPayloadFactory.apply(first);
    }
    String callerPrincipal = resolveCallerPrincipal();
    JobEntity parent = newBatchParent(callerPrincipal, idempotencyKey);
    parent.setExecutionTarget(builder.executionTarget());
    checkCreateAuthorization(parent);
    JobEntity savedParent = createJob(parent);
    UUID parentId = savedParent.getId();

    int totalItems = 0;
    int chunksInserted = 0;
    List<T> chunk = firstItems;
    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    if (builder.reducer() != null) {
      batch.setReducer(
          PayloadEncryptor.encryptJsonColumn(
              builder.reducer().json(PayloadSerializerHolder.get()),
              EncryptionHolder.encryptionActiveFor(savedParent.isEncryptedPayload()),
              EncryptionTarget.rowBound(ProtectedSurface.BATCH_REDUCER, parentId)));
    }
    batch.setTotalItems(0);
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (builder.batchProgressHook() != null) {
      batch.setProgressHook(progressPayload);
    }
    saveBatch(batch);

    createWorkflowBranches(
        parentId,
        branches,
        builder.callbackOptions(),
        builder.executionTarget(),
        savedParent.isEncryptedPayload(),
        callerPrincipal);

    while (!chunk.isEmpty() || iterator.hasNext()) {
      while (chunk.size() < builder.chunkSize() && iterator.hasNext()) {
        chunk.add(iterator.next());
      }
      if (!chunk.isEmpty()) {
        totalItems +=
            createChildChunk(
                parentId,
                builder,
                chunk,
                chunksInserted,
                callerPrincipal,
                firstPayload,
                childPayloadFactory);
        firstPayload = null;
        chunksInserted++;
        builder.invokeLocalProgressHook(parentId, totalItems, chunksInserted);
        chunk.clear();
      }
    }

    batchStore.updateBatchTotalItems(parentId, totalItems);

    if (totalItems == 0) {
      if (batchService.completeEmptyBatch(parentId, batch)) {
        wakeupService.notify(JobPriority.NORMAL, true, builder.executionTarget());
      }
      log.infof(
          "Streaming batch '%s' submitted with 0 items — completed immediately (id=%s)",
          builder.name(), parentId);
      return () -> parentId;
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
  // The internal builder crosses a public interface method that must retain transaction advice.
  @SuppressWarnings("ClassEscapesDefinedScope")
  public JobHandle submit(DefaultRecurringJobBuilder builder) {
    JOB_PAYLOAD_CONVERTER.beginPreparationScope();
    try {
      return submitPrepared(builder);
    } finally {
      JOB_PAYLOAD_CONVERTER.endPreparationScope();
    }
  }

  private JobHandle submitPrepared(DefaultRecurringJobBuilder builder) {
    if (recurringJobStore == null) {
      throw new UnsupportedOperationException(
          "Recurring job submission requires a store advertising the RecurringJobStore capability");
    }
    Cron cron = RecurringScheduler.PARSER.parse(builder.cronExpr());
    cron.validate();

    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    Instant base = effective().instant();
    Instant nextFire =
        RecurringFireTime.nextExecution(executionTime, base, builder.zone())
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
            options.priority().persistedCode(),
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
            builder.encryptedPayload(),
            builder.misfirePolicy());

    RecurringJobDefinition saved;
    try {
      saved = createOrReconcileRecurring(definition, builder.tags());
    } finally {
      discardPreparedPayloads(definition);
    }

    log.infof(
        "Recurring job submitted (id=%s, cron=%s, zone=%s, nextFire=%s)",
        saved.id(), saved.cronExpr(), saved.zoneId(), saved.nextFire());

    recurringScheduler.kick();
    return saved::id;
  }

  private RecurringJobDefinition createOrReconcileRecurring(
      RecurringJobDefinition candidate, List<String> tags) {
    Optional<RecurringJobDefinition> existing = findRecurringByBusinessKey(candidate.businessKey());
    if (existing.isPresent()) {
      return reconcileRecurring(existing.get(), candidate);
    }

    try {
      UUID saved = recurringJobStore.createRecurring(candidate);
      if (!tags.isEmpty()) {
        tagStore.insertTags(saved, tags);
      }
      return candidate;
    } catch (RatchetTransientStoreException e) {
      Optional<RecurringJobDefinition> raced = findRecurringByBusinessKey(candidate.businessKey());
      if (raced.isPresent()) {
        return reconcileRecurring(raced.get(), candidate);
      }
      throw e;
    }
  }

  private Optional<RecurringJobDefinition> findRecurringByBusinessKey(String businessKey) {
    if (businessKey == null) {
      return Optional.empty();
    }
    return recurringJobStore.findRecurringByBusinessKey(businessKey);
  }

  private RecurringJobDefinition reconcileRecurring(
      RecurringJobDefinition existing, RecurringJobDefinition candidate) {
    RecurringJobDefinition replacement = reconciledDefinition(existing, candidate);
    if (recurringDefinitionsMatch(existing, replacement)) {
      return existing;
    }
    if (!recurringJobStore.updateRecurring(existing.id(), replacement)) {
      throw new IllegalStateException(
          "Recurring master " + existing.id() + " disappeared during business-key reconciliation");
    }
    return replacement;
  }

  private static RecurringJobDefinition reconciledDefinition(
      RecurringJobDefinition existing, RecurringJobDefinition candidate) {
    boolean scheduleChanged =
        !Objects.equals(existing.cronExpr(), candidate.cronExpr())
            || !Objects.equals(existing.zoneId(), candidate.zoneId());
    Instant nextFire = scheduleChanged ? candidate.nextFire() : existing.nextFire();
    return new RecurringJobDefinition(
        existing.id(),
        candidate.cronExpr(),
        candidate.zoneId(),
        nextFire,
        existing.paused(),
        existing.pausedAt(),
        candidate.priority(),
        candidate.maxRetries(),
        candidate.backoffPolicy(),
        candidate.backoffParamMs(),
        candidate.timeoutSec(),
        candidate.payload(),
        candidate.onSuccessPayload(),
        candidate.onFailurePayload(),
        existing.businessKey(),
        candidate.resourceName(),
        candidate.executionTarget(),
        existing.createdAt(),
        existing.callerPrincipal(),
        candidate.encryptedPayload(),
        candidate.misfirePolicy());
  }

  private static boolean recurringDefinitionsMatch(
      RecurringJobDefinition existing, RecurringJobDefinition replacement) {
    return Objects.equals(existing.cronExpr(), replacement.cronExpr())
        && Objects.equals(existing.zoneId(), replacement.zoneId())
        && Objects.equals(existing.nextFire(), replacement.nextFire())
        && existing.priority() == replacement.priority()
        && existing.maxRetries() == replacement.maxRetries()
        && existing.backoffPolicy() == replacement.backoffPolicy()
        && existing.backoffParamMs() == replacement.backoffParamMs()
        && existing.timeoutSec() == replacement.timeoutSec()
        && Objects.equals(existing.payload(), replacement.payload())
        && Objects.equals(existing.onSuccessPayload(), replacement.onSuccessPayload())
        && Objects.equals(existing.onFailurePayload(), replacement.onFailurePayload())
        && Objects.equals(existing.businessKey(), replacement.businessKey())
        && Objects.equals(existing.resourceName(), replacement.resourceName())
        && Objects.equals(existing.executionTarget(), replacement.executionTarget())
        && existing.encryptedPayload() == replacement.encryptedPayload()
        && Objects.equals(existing.misfirePolicy(), replacement.misfirePolicy());
  }

  private void requireResourcePermitCapability(String resourceName, String subject) {
    if (resourceName != null && !resourceName.isBlank() && !resourcePermitCapabilityAvailable) {
      // The caller asked for concurrency gating on a named resource, but the store cannot enforce
      // it. Reject the submission rather than silently running the job with unbounded concurrency.
      throw new UnsupportedOperationException(
          subject
              + " '"
              + resourceName
              + "' but the store does not advertise the ResourcePermitStore capability; resource"
              + " concurrency gating cannot be enforced");
    }
  }

  private void requireBatchCapability() {
    if (batchStore == null) {
      throw new UnsupportedOperationException(
          "Batch submission requires a store advertising the BatchStore capability");
    }
  }

  private String resolveCallerPrincipal() {
    return CallerPrincipalResolution.resolve(callerPrincipalResolver, callerPrincipalProvider)
        .orElse(null);
  }

  private void applyOptions(JobEntity job, JobOptions opts) {
    job.setMaxRetries(opts.maxRetries());
    job.setBackoffPolicy(opts.backoffPolicy());
    job.setBackoffParamMs((int) opts.backoffParam().toMillis());
    job.setTimeoutSec(opts.timeoutSec());
  }

  private JobEntity newBatchParent(String callerPrincipal, String idempotencyKey) {
    JobEntity parent = new JobEntity();
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setStatus(JobStatus.PENDING);
    parent.setPriority(JobPriority.NORMAL);
    parent.setScheduledTime(effective().instant());
    parent.setPayload(validate(JobPayloadFactory.noop()));
    parent.setIdempotencyKey(
        idempotencyKey != null ? idempotencyKey : UUID.randomUUID().toString());
    stampCallerPrincipal(parent, callerPrincipal);
    return parent;
  }

  private void createChainSteps(
      UUID predecessorId,
      List<JobPayload> chainPayloads,
      JobOptions opts,
      String executionTarget,
      boolean parentEncrypted,
      String callerPrincipal) {
    UUID prevId = predecessorId;
    for (JobPayload chainPayload : chainPayloads) {
      JobEntity step = new JobEntity();
      step.setJobType(JobExecutionType.CHAIN_STEP);
      step.setStatus(JobStatus.PENDING);
      step.setPriority(opts.priority());
      step.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
      step.setPayload(chainPayload);
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
      stampCallerPrincipal(step, callerPrincipal);
      captureTraceContext(step);
      checkCreateAuthorization(step);

      JobEntity savedStep = createJob(step);
      prevId = savedStep.getId();
    }
  }

  /**
   * Routes one chunk to the matching child constructor. Invocation-mode chunks emit a best-effort
   * {@link BatchChunkFailureEvent} before the failure propagates (and the enclosing transaction
   * rolls the whole submission back).
   */
  private <T extends Serializable> int createChildChunk(
      UUID parentId,
      DefaultStreamingBatchBuilder<T> builder,
      List<T> chunk,
      int chunkIndex,
      String callerPrincipal,
      JobPayload firstPayload,
      Function<T, JobPayload> childPayloadFactory) {
    try {
      return createStreamingChildJobs(
          parentId, builder, chunk, callerPrincipal, firstPayload, childPayloadFactory);
    } catch (RuntimeException e) {
      if (builder instanceof InvocationStreamingState<?> && eventPublisher != null) {
        eventPublisher.publish(
            new BatchChunkFailureEvent(
                parentId,
                null,
                null,
                JobType.BATCH,
                JobPriority.NORMAL,
                null,
                chunkIndex,
                chunk.size(),
                sanitizeError(e)));
      }
      throw e;
    }
  }

  private <T extends Serializable> JobPayload streamingPayload(
      DefaultStreamingBatchBuilder<T> builder, T item) {
    JobPayload prepared =
        builder instanceof InvocationStreamingState<T> state
            ? validateRunnable(
                JobPayloadFactory.requireTaskArguments(
                    JobPayloadFactory.fromInvocation(state.invocationFactory().apply(item))))
            : payload(builder.processingAction(), List.of(item));
    if (builder.reducer() != null) builder.reducer().validateChild(prepared, classPolicy);
    return prepared;
  }

  private String sanitizeError(Throwable failure) {
    if (errorSanitizer != null) {
      try {
        String sanitized = errorSanitizer.sanitize(failure);
        if (sanitized != null) {
          return sanitized;
        }
      } catch (Throwable sanitizerError) {
        // Fall back to the exception class name without exposing the raw message.
      }
    }
    return failure.getClass().getName();
  }

  private <T extends Serializable> int createStreamingChildJobs(
      UUID parentId,
      DefaultStreamingBatchBuilder<T> builder,
      List<T> items,
      String callerPrincipal,
      JobPayload firstPayload,
      Function<T, JobPayload> payloadFactory) {
    List<JobEntity> children = new ArrayList<>(items.size());
    for (int index = 0; index < items.size(); index++) {
      T item = items.get(index);
      JobEntity child = new JobEntity();
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setStatus(JobStatus.PENDING);
      child.setPriority(JobPriority.NORMAL);
      child.setScheduledTime(effective().instant());
      JobPayload partialPayload =
          index == 0 && firstPayload != null ? firstPayload : payloadFactory.apply(item);
      child.setPayload(partialPayload);
      child.setIdempotencyKey(UUID.randomUUID().toString());
      child.setDependsOn(parentId);
      child.setExecutionTarget(builder.executionTarget());
      child.setResourceName(builder.childResourceName());
      applyOptions(child, builder.childOptions());
      stampCallerPrincipal(child, callerPrincipal);
      checkCreateAuthorization(child);
      children.add(child);
    }
    bulkInsertJobs(children);
    return children.size();
  }

  private JobBulkStore bulkStore() {
    requireBulkStore();
    return jobBulkStore;
  }

  private void requireBulkStore() {
    if (jobBulkStore == null) {
      throw new IllegalStateException(
          "Batch submission requires a JobBulkStore; use the CDI constructor or pass a store that"
              + " implements JobBulkStore.");
    }
  }

  private record BranchPayloads(WorkflowBranch branch, JobPayload payload, JobPayload expression) {}

  private List<BranchPayloads> prepareWorkflowBranches(
      List<WorkflowBranch> branches, WorkflowBranch resultSlotBranch) {
    requireWorkflowConditionCapability(branches);
    List<BranchPayloads> prepared = new ArrayList<>(branches.size());
    for (WorkflowBranch branch : branches) {
      JobPayload task =
          payload(
              branch.task(),
              branch == resultSlotBranch
                  && branch.condition().type() == ConditionType.BATCH_SUCCESS);
      Serializable expression = branch.condition().expression();
      JobPayload condition =
          expression instanceof JobInvocation invocation
              ? validate(JobPayloadFactory.fromInvocation(invocation))
              : expression instanceof SerializablePredicate<?>
                      || expression instanceof SerializableFunction<?, ?>
                  ? validate(JobPayloadFactory.deferredConditionPayload(expression))
                  : null;
      prepared.add(new BranchPayloads(branch, task, condition));
    }
    return prepared;
  }

  private void createWorkflowBranches(
      UUID parentId,
      List<BranchPayloads> branches,
      JobOptions options,
      String executionTarget,
      boolean parentEncrypted,
      String callerPrincipal) {
    for (int definitionOrder = 0; definitionOrder < branches.size(); definitionOrder++) {
      persistWorkflowBranch(
          prepareWorkflowBranch(
              parentId,
              branches.get(definitionOrder),
              definitionOrder,
              options,
              executionTarget,
              parentEncrypted,
              callerPrincipal));
    }
  }

  private PreparedWorkflowBranch prepareWorkflowBranch(
      UUID parentId,
      BranchPayloads prepared,
      int definitionOrder,
      JobOptions options,
      String executionTarget,
      boolean parentEncrypted,
      String callerPrincipal) {
    WorkflowBranch branch = prepared.branch();
    JobEntity branchJob = new JobEntity();
    branchJob.setId(UuidV7Factory.create());
    branchJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    branchJob.setStatus(JobStatus.PENDING);
    branchJob.setPriority(options.priority());
    applyOptions(branchJob, options);
    branchJob.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    branchJob.setPayload(prepared.payload());
    branchJob.setEncryptedPayload(parentEncrypted);
    branchJob.setIdempotencyKey(UUID.randomUUID().toString());
    branchJob.setDependsOn(parentId);
    branchJob.setExecutionTarget(executionTarget);
    stampCallerPrincipal(branchJob, callerPrincipal);
    checkCreateAuthorization(branchJob);

    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setParentJobId(parentId);
    condition.setChildJobId(branchJob.getId());
    condition.setConditionType(branch.condition().type());
    condition.setConditionPriority(branch.condition().priority());
    condition.setDefinitionOrder(definitionOrder);
    if (branch.condition().expression() != null) {
      Serializable expr = branch.condition().expression();
      if (expr instanceof SerializablePredicate<?>
          || expr instanceof SerializableFunction<?, ?>
          || expr instanceof JobInvocation) {
        JobPayload p = prepared.expression();
        // The predicate belongs to the parent job, binds the parent id, and follows the parent's
        // encryption opt-in: an opted-in workflow encrypts its predicate even when the global
        // switch is off.
        try {
          condition.setConditionExpression(
              PayloadEncryptor.encryptArgs(
                  JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(p),
                  EncryptionHolder.encryptionActiveFor(parentEncrypted),
                  EncryptionTarget.predicate(parentId)));
        } finally {
          JOB_PAYLOAD_CONVERTER.discardPreparedSerialization(p);
        }
      } else {
        condition.setConditionExpression(expr.toString());
      }
    }
    return new PreparedWorkflowBranch(branchJob, condition);
  }

  private record PreparedWorkflowBranch(JobEntity job, WorkflowConditionEntity condition) {}

  private void persistWorkflowBranch(PreparedWorkflowBranch branch) {
    createJob(branch.job());
    workflowConditionStore.saveCondition(branch.condition());
  }

  private void requireWorkflowConditionCapability(List<WorkflowBranch> branches) {
    if (workflowConditionStore == null && !branches.isEmpty()) {
      throw new UnsupportedOperationException(
          "Workflow branch scheduling requires a store advertising the WorkflowConditionStore"
              + " capability");
    }
  }

  private JobPayload payload(Serializable callback) {
    return payload(callback, false);
  }

  private JobPayload payload(Serializable callback, boolean allowResultSlot) {
    JobPayload payload = resolvePayload(callback);
    if (allowResultSlot && List.of(0).equals(payload.runtimeArgIndexes()))
      return validateRunnable(payload);
    return validateRunnable(JobPayloadFactory.requireTaskArguments(payload));
  }

  @SuppressWarnings("unchecked")
  private <T extends Serializable> List<JobPayload> prepareReducingInvocationStream(
      DefaultStreamingBatchBuilder<T> builder) {
    if (!(builder instanceof InvocationStreamingState<?>)) return null;
    InvocationStreamingState<T> state = (InvocationStreamingState<T>) builder;
    // A trusted factory may choose a different target per item. Stage its invocations once so
    // every declared result type is validated before any row is written, including on MongoDB.
    List<T> items = state.stream().toList();
    List<JobPayload> payloads = new ArrayList<>(items.size());
    for (T item : items) {
      JobPayload prepared =
          validateRunnable(
              JobPayloadFactory.requireTaskArguments(
                  JobPayloadFactory.fromInvocation(state.invocationFactory().apply(item))));
      state.reducer().validateChild(prepared, classPolicy);
      payloads.add(prepared);
    }
    state.fromStream(items.stream());
    return payloads;
  }

  private void validateReducingChildren(DefaultBatchBuilder builder) {
    for (DefaultBatchBuilder.ChildSpec child : builder.children()) {
      builder.reducer().validateChild(child.payload(), classPolicy);
    }
  }

  private void validateReducingChildren(DefaultStreamingBatchBuilder<?> builder) {
    if (builder instanceof InvocationStreamingState<?>) return;
    builder.reducer().validateChild(resolvePayload(builder.processingAction()), classPolicy);
  }

  private JobPayload runtimePayload(Serializable callback) {
    return validate(resolvePayload(callback));
  }

  private JobPayload resolvePayload(Serializable callback) {
    // Pre-resolved invocations (from InvocationSubmissionService facades) skip lambda resolution;
    // they still run through the same validation and class-policy gate below.
    if (callback instanceof InvocationAdapter adapter) {
      return JobPayloadFactory.fromInvocation(adapter.invocation());
    }
    return JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback));
  }

  private JobPayload payload(Serializable callback, List<Object> runtimeArguments) {
    return validateRunnable(
        JobPayloadFactory.requireTaskArguments(
            JobPayloadFactory.fromInvocation(
                jobInvocationResolver.resolve(callback, runtimeArguments))));
  }

  private JobPayload validateRunnable(JobPayload payload) {
    JobPayload validated = validate(payload);
    try {
      payloadInvoker.validateTransactionalSubmission(validated, transactionalCompletionAvailable);
      return validated;
    } catch (RuntimeException failure) {
      JOB_PAYLOAD_CONVERTER.discardPreparedSerialization(validated);
      throw failure;
    }
  }

  private JobPayload validate(JobPayload payload) {
    // Persist-time ClassPolicy gate: mirrors WorkflowConditionEvaluator and JobSecurityValidator
    // at execution time. Closes the TOCTOU window between persistence and execution — a malicious
    // lambda's target class is rejected before reaching the database, not after a worker dequeues
    // it. Framework coordination payloads (the batch-parent noop) target an internal placeholder,
    // never run user code, and so are not subject to the application allowlist — gating them would
    // break batch submission for any app that allowlists only its own packages. @Recurring
    // annotation payloads similarly target a framework dispatch shim; the shim validates the real
    // annotated bean class against ClassPolicy before dispatching.
    if (classPolicy != null
        && payload != null
        && payload.target() != null
        && !JobPayloadFactory.isCoordinationPlaceholder(payload)
        && !JobPayloadFactory.isRecurringDispatchShim(payload)
        && !classPolicy.isAllowed(payload.target())) {
      JOB_PAYLOAD_CONVERTER.discardPreparedSerialization(payload);
      throw new SecurityException(
          "Class " + payload.target() + " is not allowed for job execution.");
    }
    payload = JobPayloadFactory.evaluateArguments(payload);
    payloadValidator.validateAtCreation(payload);
    return payload;
  }

  private JobEntity createJob(JobEntity job) {
    try {
      return jobCrudStore.create(job);
    } finally {
      discardPreparedPayloads(job);
    }
  }

  private void bulkInsertJobs(List<JobEntity> jobs) {
    try {
      bulkStore().bulkInsert(jobs);
    } finally {
      jobs.forEach(DefaultJobCreationService::discardPreparedPayloads);
    }
  }

  private void saveBatch(BatchEntity batch) {
    try {
      batchStore.saveBatch(batch);
    } finally {
      discardPreparedPayload(batch.getProgressHook());
    }
  }

  private static void discardPreparedPayloads(JobEntity job) {
    discardPreparedPayload(job.getPayload());
    discardPreparedPayload(job.getOnSuccessPayload());
    discardPreparedPayload(job.getOnFailurePayload());
  }

  private static void discardPreparedPayloads(RecurringJobDefinition definition) {
    discardPreparedPayload(definition.payload());
    discardPreparedPayload(definition.onSuccessPayload());
    discardPreparedPayload(definition.onFailurePayload());
  }

  private static void discardPreparedPayload(JobPayload payload) {
    JOB_PAYLOAD_CONVERTER.discardPreparedSerialization(payload);
  }

  private void stampCallerPrincipal(JobEntity job, String callerPrincipal) {
    if (callerPrincipal != null) {
      job.setCallerPrincipal(callerPrincipal);
    }
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
            saved.getRecurringMasterId(),
            saved.getPublicJobType(),
            saved.getPriority(),
            null,
            signalKey,
            signalTimeout);
    AfterCommitActions.runOrFail(
        afterCommitRegistrar,
        event.getClass().getSimpleName(),
        () -> eventPublisher.publish(event));
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
