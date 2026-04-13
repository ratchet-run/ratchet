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

  public void markBatchChildFailed(JobEntity job) {
    batchService.markChildFailed(job);
  }

  public void markBatchChildSucceeded(JobEntity job) {
    batchService.markChildSucceeded(job);
  }

  public void cancelChain(JobEntity job) {
    workflowScheduler.cancelChain(job);
  }

  public void scheduleNext(JobEntity job) {
    workflowScheduler.scheduleNext(job);
  }

  public void moveToDlq(JobEntity job, Throwable ex) {
    deadLetterService.moveToDlq(job, ex);
  }

  public void handleJobSuccess(JobEntity job) {
    switch (job.getJobType()) {
      case BATCH_CHILD -> markBatchChildSucceeded(job);
      case SINGLE, CHAIN_STEP, WORKFLOW_BRANCH -> scheduleNext(job);
      default -> {
        // No additional post-success work for recurring or system jobs.
      }
    }
    // Wake the poller so it picks up any newly created jobs (workflow branches, chain steps)
    pollerScheduler.wakeup();
  }

  public void handlePermanentFailure(JobEntity job, Throwable ex) {
    switch (job.getJobType()) {
      case BATCH_CHILD -> markBatchChildFailed(job);
      case SINGLE, CHAIN_STEP, WORKFLOW_BRANCH -> {
        moveToDlq(job, ex);
        scheduleNext(job);
      }
      default -> moveToDlq(job, ex);
    }
    // Wake the poller so it picks up any newly created jobs (workflow branches, chain steps)
    pollerScheduler.wakeup();
  }
}
