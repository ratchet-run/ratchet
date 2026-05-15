package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.WorkflowBranchTriggeredEvent;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Extends {@link ChainScheduler} with conditional branching. Falls back to linear chaining when no
 * workflow conditions are defined.
 *
 * <p>Internal RI service. Public methods inherit the class-level Jakarta Transactions {@code
 * REQUIRED} behavior so workflow branch mutations commit or roll back with the caller's scheduler
 * operation.
 */
@ApplicationScoped
@Transactional
public class WorkflowScheduler extends ChainScheduler {

  private static final Logger log = Logger.getLogger(WorkflowScheduler.class);

  private final WorkflowConditionStore conditionStore;
  private final WorkflowConditionEvaluator conditionEvaluator;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobTerminalStore jobTerminalStore;
  private final Clock clock;

  protected WorkflowScheduler() {
    super();
    this.conditionStore = null;
    this.conditionEvaluator = null;
    this.jobBatchStatusStore = null;
    this.jobTerminalStore = null;
    this.clock = null;
  }

  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator) {
    this(
        jobCrudStore,
        jobBatchStatusStore,
        jobTerminalStore,
        conditionStore,
        conditionEvaluator,
        Clock.systemUTC());
  }

  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator,
      Clock clock) {
    this(
        jobCrudStore,
        jobBatchStatusStore,
        jobTerminalStore,
        conditionStore,
        conditionEvaluator,
        clock,
        null);
  }

  @Inject
  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator,
      Clock clock,
      InternalEventPublisher eventPublisher) {
    super(jobCrudStore, jobTerminalStore, clock, eventPublisher);
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobTerminalStore = jobTerminalStore;
    this.conditionStore = conditionStore;
    this.conditionEvaluator = conditionEvaluator;
    this.clock = clock;
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   */
  @Override
  public void cancelChain(JobEntity parentJob) {
    super.cancelChain(parentJob);

    List<WorkflowConditionEntity> conditions =
        conditionStore.findConditionsByParentJobId(parentJob.getId());
    Map<UUID, JobEntity> childJobs = loadChildJobs(conditions);

    AtomicInteger canceledCount = new AtomicInteger(0);
    for (WorkflowConditionEntity condition : conditions) {
      JobEntity childJob = childJobs.get(condition.getChildJobId());
      if (childJob == null
          || (childJob.getStatus() != JobStatus.PENDING
              && childJob.getStatus() != JobStatus.WAITING)) {
        continue;
      }
      // Terminal CANCELED transition: cancelJob runs DELETE hot + UPDATE cold +
      // DELETE bkres atomically. setStatus()+save() is rejected by the hot guard.
      if (jobTerminalStore.cancelJob(childJob.getId())) {
        canceledCount.incrementAndGet();
        log.infof(
            "Canceled workflow branch job %s due to parent job %s failure",
            childJob.getId(), parentJob.getId());
      }
    }

    if (canceledCount.get() > 0) {
      log.infof(
          "Canceled %s workflow branch jobs for failed parent job %s",
          canceledCount, parentJob.getId());
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   */
  @Override
  public boolean scheduleNext(JobEntity parentJob) {
    List<WorkflowConditionEntity> conditions =
        conditionStore.findConditionsByParentJobId(parentJob.getId());

    if (conditions.isEmpty()) {
      // Fall back to original linear chaining behavior
      if (parentJob.getStatus() == JobStatus.FAILED) {
        super.cancelChain(parentJob);
        return false;
      } else {
        return super.scheduleNext(parentJob);
      }
    }

    log.infof("Evaluating %s workflow conditions for job %s", conditions.size(), parentJob.getId());

    Map<UUID, JobEntity> childJobs = loadChildJobs(conditions);
    WorkflowConditionEntity scheduledCondition = null;
    for (WorkflowConditionEntity condition : conditions) {
      try {
        if (conditionEvaluator.evaluate(condition, parentJob)) {
          boolean scheduled = scheduleChildJob(condition, parentJob, childJobs);
          if (scheduled) {
            scheduledCondition = condition;
            log.infof(
                "Scheduled workflow branch job %s after condition evaluation (type: %s, priority: %s)",
                condition.getChildJobId(),
                condition.getConditionType(),
                condition.getConditionPriority());
            break;
          }
        }
      } catch (Exception e) {
        log.errorf(
            e,
            "Unexpected exception evaluating workflow condition %s for job %s: %s",
            condition.getId(),
            parentJob.getId(),
            e.getMessage());
        throw failWorkflowCondition(parentJob, e);
      }
    }

    cancelUnscheduledBranches(conditions, scheduledCondition, childJobs);

    if (scheduledCondition != null) {
      publishWorkflowBranchTriggered(parentJob, scheduledCondition);
      log.infof(
          "Scheduled workflow branch job %s for parent job %s",
          scheduledCondition.getChildJobId(), parentJob.getId());
      return true;
    }

    log.infof("No workflow conditions met for job %s", parentJob.getId());
    if (parentJob.getStatus() == JobStatus.FAILED) {
      super.cancelChain(parentJob);
    }
    return false;
  }

  private IllegalStateException failWorkflowCondition(JobEntity parentJob, Exception cause) {
    String error = "Workflow condition evaluation failed: " + cause.getMessage();

    if (!jobBatchStatusStore.tryPickUpJob(
        parentJob.getId(), DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
      return new IllegalStateException(
          "Workflow condition evaluation failed, and parent "
              + parentJob.getId()
              + " could not be claimed for terminal failure recovery",
          cause);
    }

    boolean marked =
        jobTerminalStore.markJobFailedTerminal(parentJob.getId(), error, parentJob.getAttempts());
    if (!marked) {
      resetWorkflowFailurePickup(parentJob.getId(), cause);
      return new IllegalStateException(
          "Workflow condition evaluation failed, and parent "
              + parentJob.getId()
              + " could not be marked failed",
          cause);
    }

    parentJob.setStatus(JobStatus.FAILED);
    parentJob.setLastError(error);
    cancelChain(parentJob);
    return new IllegalStateException(error, cause);
  }

  private void resetWorkflowFailurePickup(UUID parentId, Exception cause) {
    try {
      if (jobBatchStatusStore.resetRunningJob(
          parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
        return;
      }
    } catch (RuntimeException resetFailure) {
      resetFailure.addSuppressed(cause);
      throw resetFailure;
    }

    IllegalStateException failure =
        new IllegalStateException(
            "Workflow parent "
                + parentId
                + " synthetic pickup could not be reset after terminal transition failure");
    failure.addSuppressed(cause);
    throw failure;
  }

  private void cancelUnscheduledBranches(
      List<WorkflowConditionEntity> conditions,
      WorkflowConditionEntity scheduledCondition,
      Map<UUID, JobEntity> childJobs) {
    UUID scheduledChildId = scheduledCondition == null ? null : scheduledCondition.getChildJobId();
    for (WorkflowConditionEntity condition : conditions) {
      if (Objects.equals(condition.getChildJobId(), scheduledChildId)) {
        continue;
      }
      JobEntity childJob = childJobs.get(condition.getChildJobId());
      if (childJob == null
          || (childJob.getStatus() != JobStatus.PENDING
              && childJob.getStatus() != JobStatus.WAITING)) {
        continue;
      }
      if (jobTerminalStore.cancelJob(childJob.getId())) {
        log.infof(
            "Canceled unmatched workflow branch job %s for condition %s",
            childJob.getId(), condition.getId());
      }
    }
  }

  @SuppressWarnings("java:S1172") // parentJob reserved for future parent context logging
  private boolean scheduleChildJob(
      WorkflowConditionEntity condition, JobEntity parentJob, Map<UUID, JobEntity> childJobs) {
    JobEntity childJob = childJobs.get(condition.getChildJobId());
    if (childJob == null) {
      log.warnf(
          "Child job %s not found for workflow condition %s",
          condition.getChildJobId(), condition.getId());
      return false;
    }
    return scheduleIfPending(condition, childJob);
  }

  @SuppressWarnings("java:S1172") // condition reserved for future context logging
  private boolean scheduleIfPending(WorkflowConditionEntity condition, JobEntity childJob) {
    if (childJob.getStatus() != JobStatus.PENDING && childJob.getStatus() != JobStatus.WAITING) {
      log.warnf(
          "Child job %s is not PENDING or WAITING (current: %s), cannot schedule",
          childJob.getId(), childJob.getStatus());
      return false;
    }

    childJob.setScheduledTime(effective().instant());
    childJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    jobCrudStore.save(childJob);
    return true;
  }

  private void publishWorkflowBranchTriggered(
      JobEntity parentJob, WorkflowConditionEntity condition) {
    if (eventPublisher == null) {
      return;
    }
    eventPublisher.publish(
        new WorkflowBranchTriggeredEvent(
            parentJob.getId(),
            parentJob.getBusinessKey(),
            parentJob.getPublicJobType(),
            parentJob.getPriority(),
            parentJob.getPickedBy(),
            describeCondition(condition),
            condition.getChildJobId()));
  }

  private String describeCondition(WorkflowConditionEntity condition) {
    if (condition.getConditionExpression() != null
        && !condition.getConditionExpression().isBlank()) {
      return condition.getConditionExpression();
    }
    return condition.getConditionType().name();
  }

  private Map<UUID, JobEntity> loadChildJobs(List<WorkflowConditionEntity> conditions) {
    List<UUID> childIds =
        conditions.stream().map(WorkflowConditionEntity::getChildJobId).distinct().toList();
    if (childIds.isEmpty()) {
      return Map.of();
    }
    return jobCrudStore.findByIds(childIds).stream()
        .collect(Collectors.toMap(JobEntity::getId, Function.identity(), (left, right) -> left));
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
