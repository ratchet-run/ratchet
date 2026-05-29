package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.RetryBufferDrainer;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;

/** Coordinates job submission, retry-buffer draining, and shutdown reset. */
@ApplicationScoped
public class JobExecutionCoordinator {

  private static final Logger log = Logger.getLogger(JobExecutionCoordinator.class);

  private final JobSubmissionService jobSubmissionService;
  private final JobStateManager jobStateManager;
  private final RetryBufferDrainer retryBufferDrainer;
  private final JobExecutorService jobExecutorService;

  protected JobExecutionCoordinator() {
    this.jobSubmissionService = null;
    this.jobStateManager = null;
    this.retryBufferDrainer = null;
    this.jobExecutorService = null;
  }

  @Inject
  public JobExecutionCoordinator(
      JobSubmissionService jobSubmissionService,
      JobStateManager jobStateManager,
      RetryBufferDrainer retryBufferDrainer,
      JobExecutorService jobExecutorService) {
    this.jobSubmissionService = jobSubmissionService;
    this.jobStateManager = jobStateManager;
    this.retryBufferDrainer = retryBufferDrainer;
    this.jobExecutorService = jobExecutorService;
  }

  public void submit(JobEntity job) {
    jobSubmissionService.submit(job);
  }

  public void submit(JobClaimDto claim) {
    jobSubmissionService.submit(claim);
  }

  public void initRetryBufferDrainer() {
    retryBufferDrainer.start();
  }

  public void shutdown() {
    retryBufferDrainer.shutdown();
    int activeExecutions = jobExecutorService.shutdownActiveExecutions();
    if (activeExecutions > 0) {
      log.warnf(
          "JobExecutionCoordinator shutdown - leaving RUNNING jobs unchanged because %s active "
              + "execution(s) did not stop; orphan recovery will handle them",
          activeExecutions);
      return;
    }
    int reset = jobStateManager.resetRunningJobsForNode();
    log.infof("JobExecutionCoordinator shutdown - reset %s RUNNING jobs to PENDING", reset);
  }
}
