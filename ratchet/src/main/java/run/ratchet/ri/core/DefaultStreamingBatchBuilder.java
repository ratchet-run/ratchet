package run.ratchet.ri.core;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.SerializableCheckedConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.StreamingBatchContext;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.jboss.logging.Logger;

/** {@inheritDoc} */
@SuppressWarnings("unchecked")
public class DefaultStreamingBatchBuilder<T extends Serializable>
    implements StreamingBatchBuilder<T> {

  private static final Logger log = Logger.getLogger(DefaultStreamingBatchBuilder.class);
  private static final int MIN_CHUNK_SIZE = 1;
  private static final int DEFAULT_CHUNK_SIZE = 100;

  private final String name;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final JobWakeupService wakeupService;
  private final JobInvocationResolver jobInvocationResolver;

  private final List<WorkflowBranch> workflowBranches = new ArrayList<>();
  private Stream<T> stream;
  private SerializableCheckedConsumer<T> action;
  private int chunkSize = DEFAULT_CHUNK_SIZE;
  private Consumer<StreamingBatchContext> localProgressHook;
  private SerializableConsumer<BatchContext> batchProgressHook;

  DefaultStreamingBatchBuilder(
      String name,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService) {
    this(
        name,
        jobCrudStore,
        batchStore,
        workflowConditionStore,
        wakeupService,
        new DefaultJobInvocationResolver());
  }

  DefaultStreamingBatchBuilder(
      String name,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      JobInvocationResolver jobInvocationResolver) {
    this.name = name;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
    this.jobInvocationResolver = jobInvocationResolver;
  }

  @Override
  public <U extends Serializable> StreamingBatchBuilder<U> fromStream(Stream<U> stream) {
    DefaultStreamingBatchBuilder<U> cast = (DefaultStreamingBatchBuilder<U>) this;
    cast.stream = stream;
    return cast;
  }

  @Override
  public StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action) {
    this.action = action;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> withChunkSize(int size) {
    if (size < MIN_CHUNK_SIZE) {
      throw new IllegalArgumentException("Chunk size must be greater than zero");
    }
    this.chunkSize = size;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook) {
    this.localProgressHook = hook;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook) {
    this.batchProgressHook = hook;
    return this;
  }

  @Override
  public JobHandle start() {
    if (stream == null) {
      throw new IllegalStateException("Stream must be set via fromStream() before calling start()");
    }
    if (action == null) {
      throw new IllegalStateException(
          "Processing action must be set via process() before calling start()");
    }

    JobEntity parent = new JobEntity();
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setStatus(JobStatus.PENDING);
    parent.setPriority(JobPriority.NORMAL);
    parent.setScheduledTime(Instant.now());
    parent.setPayload(JobPayloadFactory.noop());
    parent.setIdempotencyKey(UUID.randomUUID().toString());
    JobEntity savedParent = jobCrudStore.save(parent);
    Long parentId = savedParent.getId();

    int totalItems = 0;
    int chunksInserted = 0;
    List<T> chunk = new ArrayList<>(chunkSize);

    try {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        chunk.add(iterator.next());
        if (chunk.size() >= chunkSize) {
          totalItems += createChildJobs(parentId, chunk);
          chunksInserted++;
          invokeLocalProgressHook(parentId, totalItems, chunksInserted);
          chunk.clear();
        }
      }

      if (!chunk.isEmpty()) {
        totalItems += createChildJobs(parentId, chunk);
        chunksInserted++;
        invokeLocalProgressHook(parentId, totalItems, chunksInserted);
      }
    } finally {
      stream.close();
    }

    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(totalItems);
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (batchProgressHook != null) {
      batch.setProgressHook(payload(batchProgressHook));
    }
    batchStore.saveBatch(batch);

    for (WorkflowBranch branch : workflowBranches) {
      createWorkflowBranch(parentId, branch);
    }

    wakeupService.notifyIfNeeded(JobExecutionType.BATCH_PARENT, JobPriority.NORMAL, Duration.ZERO);

    log.infof("Streaming batch '%s' submitted with %s items (id=%s)", name, totalItems, parentId);
    return () -> parentId;
  }

  @Override
  public StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchSuccess(), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchFailure(), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchCustom(condition), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.failureCount(maxFailures), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.successRate(minRate), next));
    return this;
  }

  private int createChildJobs(Long parentId, List<T> items) {
    int count = 0;
    for (T item : items) {
      JobEntity child = new JobEntity();
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setStatus(JobStatus.PENDING);
      child.setPriority(JobPriority.NORMAL);
      child.setScheduledTime(Instant.now());
      child.setPayload(payload(action, List.of(item)));
      child.setIdempotencyKey(UUID.randomUUID().toString());
      child.setDependsOn(parentId);
      jobCrudStore.save(child);
      count++;
    }
    return count;
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
    return JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback));
  }

  private JobPayload payload(Serializable callback, List<Object> runtimeArguments) {
    return JobPayloadFactory.fromInvocation(
        jobInvocationResolver.resolve(callback, runtimeArguments));
  }

  private void invokeLocalProgressHook(Long batchId, int processedItems, int chunksInserted) {
    if (localProgressHook == null) {
      return;
    }

    try {
      localProgressHook.accept(new StreamingBatchContext(batchId, processedItems, chunksInserted));
    } catch (Exception e) {
      log.warnf("Streaming progress hook threw exception: %s", e.getMessage());
    }
  }
}
