package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

/**
 * Provides a simplified API for common job lifecycle operations, encapsulating the coordination
 * between multiple scheduler services.
 *
 * <p><b>Why this facade exists:</b> {@link JobTask} is the core execution engine that needs to
 * coordinate batch progress tracking, workflow scheduling, and dead letter queue management after
 * each job completes. Without this facade, JobTask would need to inject and coordinate three
 * separate services directly, duplicating routing logic (e.g., "if batch child, do X; if chain
 * step, do Y") across multiple methods. This facade centralizes that routing into {@link
 * #handleJobSuccess} and {@link #handlePermanentFailure}, keeping JobTask focused on execution
 * mechanics.
 *
 * @see JobTask
 * @see ExecutionObserver
 */
@ApplicationScoped
@Transactional
public class PostExecutionHandler {

  private final BatchService batchService;
  private final WorkflowScheduler workflowScheduler;
  private final DeadLetterService deadLetterService;
  private final PollerScheduler pollerScheduler;

  // Required by CDI proxy
  protected PostExecutionHandler() {
    this.batchService = null;
    this.workflowScheduler = null;
    this.deadLetterService = null;
    this.pollerScheduler = null;
  }

  @Inject
  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler) {
    this.batchService = batchService;
    this.workflowScheduler = workflowScheduler;
    this.deadLetterService = deadLetterService;
    this.pollerScheduler = pollerScheduler;
  }

  /**
   * Marks a batch child job as failed and updates batch progress.
   *
   * @param job the child job entity that failed
   */
  public void markBatchChildFailed(JobEntity job) {
    batchService.markChildFailed(job);
  }

  /**
   * Marks a batch child job as succeeded and updates batch progress.
   *
   * @param job the child job entity that succeeded
   */
  public void markBatchChildSucceeded(JobEntity job) {
    batchService.markChildSucceeded(job);
  }

  /**
   * Cancels all remaining jobs in a chain when one step fails.
   *
   * @param job the chain step that failed
   */
  public void cancelChain(JobEntity job) {
    workflowScheduler.cancelChain(job);
  }

  /**
   * Schedules the next job in a workflow sequence.
   *
   * @param job the job that just completed successfully
   */
  public void scheduleNext(JobEntity job) {
    workflowScheduler.scheduleNext(job);
  }

  /**
   * Moves a job to the dead letter queue after permanent failure.
   *
   * @param job the job that permanently failed
   * @param ex the exception that caused the final failure
   */
  public void moveToDlq(JobEntity job, Throwable ex) {
    deadLetterService.moveToDlq(job, ex);
  }

  /**
   * Handles successful completion of a job based on its type.
   *
   * <p>Routes to the appropriate handler based on job type:
   *
   * <ul>
   *   <li>BATCH_CHILD - marks batch child as succeeded
   *   <li>CHAIN_STEP, WORKFLOW_BRANCH - schedules next workflow step
   *   <li>Other types - no additional handling required
   * </ul>
   *
   * @param job the job that completed successfully
   */
  public void handleJobSuccess(JobEntity job) {
    switch (job.getJobType()) {
      case BATCH_CHILD -> markBatchChildSucceeded(job);
      default -> scheduleNext(job);
    }
    // Wake the poller so it picks up any newly created jobs (workflow branches, chain steps)
    pollerScheduler.wakeup();
  }

  /**
   * Handles permanent failure of a job based on its type.
   *
   * <p>Routes to the appropriate handler based on job type:
   *
   * <ul>
   *   <li>BATCH_CHILD - marks batch child as failed
   *   <li>CHAIN_STEP, WORKFLOW_BRANCH - cancels the chain
   *   <li>Other types - moves to DLQ
   * </ul>
   *
   * @param job the job that permanently failed
   * @param ex the exception that caused the failure
   */
  public void handlePermanentFailure(JobEntity job, Throwable ex) {
    switch (job.getJobType()) {
      case BATCH_CHILD -> markBatchChildFailed(job);
      default -> {
        moveToDlq(job, ex);
        scheduleNext(job);
      }
    }
    // Wake the poller so it picks up any newly created jobs (workflow branches, chain steps)
    pollerScheduler.wakeup();
  }
}
