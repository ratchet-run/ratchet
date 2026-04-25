package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;

/**
 * Extends {@link ChainScheduler} with conditional branching. Falls back to linear chaining when no
 * workflow conditions are defined.
 */
@ApplicationScoped
@Transactional
public class WorkflowScheduler extends ChainScheduler {

  private static final Logger log = Logger.getLogger(WorkflowScheduler.class);

  private final WorkflowConditionStore conditionStore;
  private final WorkflowConditionEvaluator conditionEvaluator;
  private final JobCrudStore jobCrudStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobTerminalStore jobTerminalStore;

  protected WorkflowScheduler() {
    super();
    this.conditionStore = null;
    this.conditionEvaluator = null;
    this.jobCrudStore = null;
    this.jobBatchStatusStore = null;
    this.jobTerminalStore = null;
  }

  @Inject
  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator) {
    super(jobCrudStore);
    this.jobCrudStore = jobCrudStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobTerminalStore = jobTerminalStore;
    this.conditionStore = conditionStore;
    this.conditionEvaluator = conditionEvaluator;
  }

  @Override
  public void cancelChain(JobEntity parentJob) {
    super.cancelChain(parentJob);

    List<WorkflowConditionEntity> conditions =
        conditionStore.findConditionsByParentJobId(parentJob.getId());

    AtomicInteger canceledCount = new AtomicInteger(0);
    for (WorkflowConditionEntity condition : conditions) {
      jobCrudStore
          .findById(condition.getChildJobId())
          .filter(job -> job.getStatus() == JobStatus.PENDING)
          .ifPresent(
              childJob -> {
                // Terminal CANCELED transition: cancelJob runs DELETE hot + UPDATE cold +
                // DELETE bkres atomically. setStatus()+save() is rejected by the hot guard.
                if (jobTerminalStore.cancelJob(childJob.getId())) {
                  canceledCount.incrementAndGet();
                  log.infof(
                      "Canceled workflow branch job %s due to parent job %s failure",
                      childJob.getId(), parentJob.getId());
                }
              });
    }

    if (canceledCount.get() > 0) {
      log.infof(
          "Canceled %s workflow branch jobs for failed parent job %s",
          canceledCount, parentJob.getId());
    }
  }

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

    WorkflowConditionEntity scheduledCondition = null;
    for (WorkflowConditionEntity condition : conditions) {
      try {
        if (conditionEvaluator.evaluate(condition, parentJob)) {
          boolean scheduled = scheduleChildJob(condition, parentJob);
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
        // Mark parent FAILED through the explicit terminal pathway. The parent is BATCH_PARENT
        // sitting at PENDING; we synthesize the picker so the gate matches before mark-failed.
        String error = "Workflow condition evaluation failed: " + e.getMessage();
        if (jobBatchStatusStore.tryPickUpJob(
            parentJob.getId(), DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
          jobTerminalStore.markJobFailedTerminal(parentJob.getId(), error, parentJob.getAttempts());
        }
        parentJob.setStatus(JobStatus.FAILED);
        parentJob.setLastError(error);
        cancelChain(parentJob);
        return false;
      }
    }

    cancelUnscheduledBranches(conditions, scheduledCondition);

    if (scheduledCondition != null) {
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

  private void cancelUnscheduledBranches(
      List<WorkflowConditionEntity> conditions, WorkflowConditionEntity scheduledCondition) {
    Long scheduledChildId = scheduledCondition == null ? null : scheduledCondition.getChildJobId();
    for (WorkflowConditionEntity condition : conditions) {
      if (Objects.equals(condition.getChildJobId(), scheduledChildId)) {
        continue;
      }
      jobCrudStore
          .findById(condition.getChildJobId())
          .filter(job -> job.getStatus() == JobStatus.PENDING)
          .ifPresent(
              childJob -> {
                if (jobTerminalStore.cancelJob(childJob.getId())) {
                  log.infof(
                      "Canceled unmatched workflow branch job %s for condition %s",
                      childJob.getId(), condition.getId());
                }
              });
    }
  }

  @SuppressWarnings("java:S1172") // parentJob reserved for future parent context logging
  private boolean scheduleChildJob(WorkflowConditionEntity condition, JobEntity parentJob) {
    return jobCrudStore
        .findById(condition.getChildJobId())
        .map(childJob -> scheduleIfPending(condition, childJob))
        .orElseGet(
            () -> {
              log.warnf(
                  "Child job %s not found for workflow condition %s",
                  condition.getChildJobId(), condition.getId());
              return false;
            });
  }

  @SuppressWarnings("java:S1172") // condition reserved for future context logging
  private boolean scheduleIfPending(WorkflowConditionEntity condition, JobEntity childJob) {
    if (childJob.getStatus() != JobStatus.PENDING) {
      log.warnf(
          "Child job %s is not in PENDING status (current: %s), cannot schedule",
          childJob.getId(), childJob.getStatus());
      return false;
    }

    childJob.setScheduledTime(Instant.now());
    childJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    jobCrudStore.save(childJob);
    return true;
  }
}
