package run.ratchet.ri.core;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/** Coordinates job submission, retry-buffer draining, and shutdown reset. */
@ApplicationScoped
public class JobExecutionCoordinator {

  private static final Logger log = Logger.getLogger(JobExecutionCoordinator.class);

  private final JobSubmissionService jobSubmissionService;
  private final JobStateManager jobStateManager;
  private final RetryBufferDrainer retryBufferDrainer;

  // Required by CDI proxy
  protected JobExecutionCoordinator() {
    this.jobSubmissionService = null;
    this.jobStateManager = null;
    this.retryBufferDrainer = null;
  }

  @Inject
  public JobExecutionCoordinator(
      JobSubmissionService jobSubmissionService,
      JobStateManager jobStateManager,
      RetryBufferDrainer retryBufferDrainer) {
    this.jobSubmissionService = jobSubmissionService;
    this.jobStateManager = jobStateManager;
    this.retryBufferDrainer = retryBufferDrainer;
  }

  /**
   * Initializes components that require database access.
   *
   * <p>This method must be called after database migrations have completed.
   */
  public void initDatabaseDependentComponents() {
    // Extension point for components that require database access at startup
  }

  /**
   * Submits a job for processing.
   *
   * @param job the {@code JobEntity} instance representing the job to be submitted
   */
  public void submit(JobEntity job) {
    jobSubmissionService.submit(job);
  }

  /**
   * Submits a job claim for processing.
   *
   * <p>This optimized method accepts a lightweight {@link JobClaimDto} instead of the full {@link
   * JobEntity}, reducing data transfer by 40-100x during the claim phase.
   *
   * @param claim the {@code JobClaimDto} instance representing the claimed job metadata
   */
  public void submit(JobClaimDto claim) {
    jobSubmissionService.submit(claim);
  }

  /**
   * Initializes the retry buffer drainer background task. This should be called during application
   * startup after dependency injection is complete.
   */
  public void initRetryBufferDrainer() {
    retryBufferDrainer.start();
  }

  /**
   * Safely shuts down the job execution coordinator and resets the state of running jobs.
   *
   * <p>Resets all jobs currently assigned to this node from RUNNING status back to PENDING,
   * allowing other cluster nodes to pick them up.
   */
  public void shutdown() {
    int reset = jobStateManager.resetRunningJobsForNode();
    log.infof("JobExecutionCoordinator shutdown - reset %s RUNNING jobs to PENDING", reset);
  }
}
