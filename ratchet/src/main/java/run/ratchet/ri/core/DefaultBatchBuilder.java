package run.ratchet.ri.core;

import run.ratchet.api.BatchBuilder;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Default implementation of {@link BatchBuilder} for the ratchet reference implementation.
 *
 * <p>Creates a BATCH_PARENT job with BATCH_CHILD jobs for each item in the collection. The parent
 * job tracks overall progress via a {@link BatchEntity}.
 */
@Transactional
public class DefaultBatchBuilder implements BatchBuilder {

  private static final Logger log = Logger.getLogger(DefaultBatchBuilder.class.getName());

  private final String name;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final JobWakeupService wakeupService;

  private final List<ChildSpec> children = new ArrayList<>();
  private final List<WorkflowBranch> workflowBranches = new ArrayList<>();
  private SerializableConsumer<BatchContext> progressHook;

  DefaultBatchBuilder(
      String name,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService) {
    this.name = name;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.wakeupService = wakeupService;
  }

  @Override
  public <T extends Serializable> BatchBuilder forEach(
      Collection<T> items, SerializableConsumer<T> action) {
    for (T item : items) {
      children.add(new ChildSpec(JobPayloadFactory.fromLambda(action, List.of(item))));
    }
    return this;
  }

  @Override
  public BatchBuilder onProgress(SerializableConsumer<BatchContext> hook) {
    this.progressHook = hook;
    return this;
  }

  @Override
  public JobHandle submit() {
    // Create parent job
    JobEntity parent = new JobEntity();
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setStatus(JobStatus.PENDING);
    parent.setPriority(JobPriority.NORMAL);
    parent.setScheduledTime(Instant.now());
    parent.setPayload(JobPayloadFactory.noop());
    parent.setIdempotencyKey(UUID.randomUUID().toString());
    JobEntity savedParent = jobCrudStore.save(parent);
    Long parentId = savedParent.getId();

    // Create batch entity for progress tracking
    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(children.size());
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    if (progressHook != null) {
      batch.setProgressHook(JobPayloadFactory.fromLambda(progressHook));
    }
    batchStore.saveBatch(batch);

    // Create child jobs
    for (ChildSpec child : children) {
      JobEntity childJob = new JobEntity();
      childJob.setJobType(JobExecutionType.BATCH_CHILD);
      childJob.setStatus(JobStatus.PENDING);
      childJob.setPriority(JobPriority.NORMAL);
      childJob.setScheduledTime(Instant.now());
      childJob.setPayload(child.payload);
      childJob.setIdempotencyKey(UUID.randomUUID().toString());
      childJob.setDependsOn(parentId);
      jobCrudStore.save(childJob);
    }

    // Create workflow branches
    for (WorkflowBranch branch : workflowBranches) {
      createWorkflowBranch(parentId, branch);
    }

    // Notify wakeup service
    wakeupService.notifyIfNeeded(JobExecutionType.BATCH_PARENT, JobPriority.NORMAL, Duration.ZERO);

    log.info(
        "Batch '"
            + name
            + "' submitted with "
            + children.size()
            + " children (id="
            + parentId
            + ")");
    return () -> parentId;
  }

  @Override
  public BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
    workflowBranches.add(WorkflowBranch.of(condition, next, description));
    return this;
  }

  @Override
  public BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchFailure(), next));
    return this;
  }

  @Override
  public BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchSuccess(), next));
    return this;
  }

  @Override
  public BatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchCustom(condition), next));
    return this;
  }

  @Override
  public BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.failureCount(maxFailures), next));
    return this;
  }

  @Override
  public BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.successRate(minRate), next));
    return this;
  }

  private void createWorkflowBranch(Long parentId, WorkflowBranch branch) {
    // Create the child job for this branch (locked until parent completes)
    JobEntity branchJob = new JobEntity();
    branchJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    branchJob.setStatus(JobStatus.PENDING);
    branchJob.setPriority(JobPriority.NORMAL);
    branchJob.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    branchJob.setPayload(JobPayloadFactory.fromLambda(branch.task()));
    branchJob.setIdempotencyKey(UUID.randomUUID().toString());
    branchJob.setDependsOn(parentId);
    JobEntity savedBranch = jobCrudStore.save(branchJob);

    // Create workflow condition linking parent to branch
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

  private record ChildSpec(JobPayload payload) {}
}
