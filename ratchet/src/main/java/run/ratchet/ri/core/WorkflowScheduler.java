package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStatusStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.List;
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
  private final JobStatusStore jobStatusStore;

  protected WorkflowScheduler() {
    super();
    this.conditionStore = null;
    this.conditionEvaluator = null;
    this.jobCrudStore = null;
    this.jobStatusStore = null;
  }

  @Inject
  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobStatusStore jobStatusStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator) {
    super(jobCrudStore);
    this.jobCrudStore = jobCrudStore;
    this.jobStatusStore = jobStatusStore;
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
                if (jobStatusStore.cancelJob(childJob.getId())) {
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

    int scheduledCount = 0;
    for (WorkflowConditionEntity condition : conditions) {
      try {
        if (conditionEvaluator.evaluate(condition, parentJob)) {
          boolean scheduled = scheduleChildJob(condition, parentJob);
          if (scheduled) {
            scheduledCount++;
            log.infof(
                "Scheduled workflow branch job %s after condition evaluation (type: %s, priority: %s)",
                condition.getChildJobId(),
                condition.getConditionType(),
                condition.getConditionPriority());
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
        if (jobStatusStore.tryPickUpJob(
            parentJob.getId(), DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
          jobStatusStore.markJobFailedTerminal(parentJob.getId(), error, parentJob.getAttempts());
        }
        parentJob.setStatus(JobStatus.FAILED);
        parentJob.setLastError(error);
        cancelChain(parentJob);
        return false;
      }
    }

    if (scheduledCount > 0) {
      log.infof(
          "Scheduled %s workflow branch jobs for parent job %s", scheduledCount, parentJob.getId());
    } else {
      log.infof(
          "No workflow conditions met for job %s, checking for linear chain", parentJob.getId());
      if (parentJob.getStatus() == JobStatus.FAILED) {
        super.cancelChain(parentJob);
        return false;
      } else {
        return super.scheduleNext(parentJob);
      }
    }
    return scheduledCount > 0;
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
