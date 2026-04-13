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

  /** Must be called after database migrations have completed. */
  public void initDatabaseDependentComponents() {
    // Extension point for components that require database access at startup
  }

  public void submit(JobEntity job) {
    jobSubmissionService.submit(job);
  }

  /** Accepts a lightweight {@link JobClaimDto}; full entity loading is deferred until execution. */
  public void submit(JobClaimDto claim) {
    jobSubmissionService.submit(claim);
  }

  public void initRetryBufferDrainer() {
    retryBufferDrainer.start();
  }

  /** Resets RUNNING jobs for this node back to PENDING so other nodes can pick them up. */
  public void shutdown() {
    int reset = jobStateManager.resetRunningJobsForNode();
    log.infof("JobExecutionCoordinator shutdown - reset %s RUNNING jobs to PENDING", reset);
  }
}
