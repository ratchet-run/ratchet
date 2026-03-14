package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates complex workflow execution by evaluating conditions and scheduling dependent jobs
 * based on parent job results. This service extends the linear chaining capabilities with
 * sophisticated conditional branching, enabling if-then-else patterns and multi-path workflows.
 *
 * <p>The WorkflowScheduler supports advanced execution patterns:
 *
 * <ul>
 *   <li><b>Conditional Branching:</b> Jobs execute only when specific conditions are met based on
 *       parent job results, status, or custom predicates
 *   <li><b>Multi-Path Execution:</b> Multiple child jobs can be scheduled from a single parent,
 *       each with different conditions
 *   <li><b>Fallback Chains:</b> If no workflow conditions match, falls back to linear chain
 *       execution for backward compatibility
 *   <li><b>Priority-Based Ordering:</b> Conditions are evaluated in priority order, allowing
 *       control over branch execution precedence
 * </ul>
 *
 * @see WorkflowConditionEntity for condition storage
 * @see WorkflowConditionEvaluator for condition logic
 * @see ChainScheduler for linear chaining support
 * @see JobExecutionType#WORKFLOW_BRANCH for branch job identification
 */
@ApplicationScoped
@Transactional
public class WorkflowScheduler extends ChainScheduler {

  private static final Logger log = Logger.getLogger(WorkflowScheduler.class.getName());

  /** Store for accessing workflow condition entities. */
  private final WorkflowConditionStore conditionStore;

  /** Evaluator service for workflow condition logic. */
  private final WorkflowConditionEvaluator conditionEvaluator;

  /** Store for job entity persistence operations. */
  private final JobCrudStore jobCrudStore;

  // Required by CDI proxy
  protected WorkflowScheduler() {
    super();
    this.conditionStore = null;
    this.conditionEvaluator = null;
    this.jobCrudStore = null;
  }

  @Inject
  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator) {
    super(jobCrudStore);
    this.jobCrudStore = jobCrudStore;
    this.conditionStore = conditionStore;
    this.conditionEvaluator = conditionEvaluator;
  }

  /**
   * Cancels workflow branch jobs when a parent job fails. This extends the original chain
   * cancellation to include all conditional branches, ensuring clean workflow termination on
   * failures.
   *
   * @param parentJob the job that failed, triggering cascade cancellation
   */
  @Override
  public void cancelChain(JobEntity parentJob) {
    // Cancel linear chain jobs first
    super.cancelChain(parentJob);

    // Cancel workflow branch jobs
    List<WorkflowConditionEntity> conditions =
        conditionStore.findConditionsByParentJobId(parentJob.getId());

    AtomicInteger canceledCount = new AtomicInteger(0);
    for (WorkflowConditionEntity condition : conditions) {
      jobCrudStore
          .findById(condition.getChildJobId())
          .filter(job -> job.getStatus() == JobStatus.PENDING)
          .ifPresent(
              childJob -> {
                childJob.setStatus(JobStatus.CANCELED);
                childJob.setLastError("Parent job failed, workflow branch canceled");
                jobCrudStore.save(childJob);
                canceledCount.incrementAndGet();

                log.info(
                    "Canceled workflow branch job "
                        + childJob.getId()
                        + " due to parent job "
                        + parentJob.getId()
                        + " failure");
              });
    }

    if (canceledCount.get() > 0) {
      log.info(
          "Canceled "
              + canceledCount
              + " workflow branch jobs for failed parent job "
              + parentJob.getId());
    }
  }

  /**
   * Schedules the next jobs in a workflow based on the completion of a parent job. This method
   * evaluates workflow conditions and schedules matching child jobs, supporting complex branching
   * logic and multi-path execution.
   *
   * @param parentJob the job that has completed, triggering workflow evaluation
   */
  @Override
  public void scheduleNext(JobEntity parentJob) {
    // Get all workflow conditions for this parent job
    List<WorkflowConditionEntity> conditions =
        conditionStore.findConditionsByParentJobId(parentJob.getId());

    if (conditions.isEmpty()) {
      // Fall back to original linear chaining behavior
      if (parentJob.getStatus() == JobStatus.FAILED) {
        super.cancelChain(parentJob);
      } else {
        super.scheduleNext(parentJob);
      }
      return;
    }

    log.info(
        "Evaluating " + conditions.size() + " workflow conditions for job " + parentJob.getId());

    int scheduledCount = 0;
    for (WorkflowConditionEntity condition : conditions) {
      try {
        if (conditionEvaluator.evaluate(condition, parentJob)) {
          boolean scheduled = scheduleChildJob(condition, parentJob);
          if (scheduled) {
            scheduledCount++;
            log.info(
                "Scheduled workflow branch job "
                    + condition.getChildJobId()
                    + " after condition evaluation (type: "
                    + condition.getConditionType()
                    + ", priority: "
                    + condition.getConditionPriority()
                    + ")");
          }
        }
      } catch (Exception e) {
        log.log(
            Level.SEVERE,
            "Unexpected exception evaluating workflow condition "
                + condition.getId()
                + " for job "
                + parentJob.getId()
                + ": "
                + e.getMessage(),
            e);
        // An unexpected exception during condition evaluation indicates a system error.
        // Fail the workflow to prevent inconsistent state.
        parentJob.setStatus(JobStatus.FAILED);
        parentJob.setLastError("Workflow condition evaluation failed: " + e.getMessage());
        jobCrudStore.save(parentJob);
        cancelChain(parentJob);
        return;
      }
    }

    if (scheduledCount > 0) {
      log.info(
          "Scheduled "
              + scheduledCount
              + " workflow branch jobs for parent job "
              + parentJob.getId());
    } else {
      log.info(
          "No workflow conditions met for job "
              + parentJob.getId()
              + ", checking for linear chain");
      // Fall back to linear chain behavior, respecting failure status
      if (parentJob.getStatus() == JobStatus.FAILED) {
        super.cancelChain(parentJob);
      } else {
        super.scheduleNext(parentJob);
      }
    }
  }

  /**
   * Schedules a child job if it's in the correct state.
   *
   * @param condition the workflow condition that was met
   * @param parentJob the parent job that completed (used for logging context)
   * @return true if the job was scheduled, false if validation failed
   */
  @SuppressWarnings("java:S1172") // parentJob reserved for future parent context logging
  private boolean scheduleChildJob(WorkflowConditionEntity condition, JobEntity parentJob) {
    return jobCrudStore
        .findById(condition.getChildJobId())
        .map(childJob -> scheduleIfPending(condition, childJob))
        .orElseGet(
            () -> {
              log.warning(
                  "Child job "
                      + condition.getChildJobId()
                      + " not found for workflow condition "
                      + condition.getId());
              return false;
            });
  }

  /**
   * Schedules a child job if it is in PENDING status.
   *
   * @param condition the workflow condition (for logging)
   * @param childJob the child job to schedule
   * @return true if scheduled, false if not in PENDING status
   */
  @SuppressWarnings("java:S1172") // condition reserved for future context logging
  private boolean scheduleIfPending(WorkflowConditionEntity condition, JobEntity childJob) {
    if (childJob.getStatus() != JobStatus.PENDING) {
      log.warning(
          "Child job "
              + childJob.getId()
              + " is not in PENDING status (current: "
              + childJob.getStatus()
              + "), cannot schedule");
      return false;
    }

    childJob.setScheduledTime(Instant.now());
    childJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    jobCrudStore.save(childJob);
    return true;
  }
}
