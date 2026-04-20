package run.ratchet.ri.core;

import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobBatchStatusStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

/** Resets jobs to PENDING for reprocessing. */
@ApplicationScoped
@Transactional
public class JobStateManager {

  private static final Logger log = Logger.getLogger(JobStateManager.class);

  private final JobBatchStatusStore jobBatchStatusStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  protected JobStateManager() {
    this.jobBatchStatusStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public JobStateManager(
      JobBatchStatusStore jobBatchStatusStore, NodeIdentityProvider nodeIdentityProvider) {
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
  }

  /**
   * Resets a RUNNING job owned by this node back to PENDING. On success, the in-memory entity is
   * updated to match.
   */
  public boolean resetJobToPending(JobEntity job) {
    if (resetJobToPending(job.getId())) {
      job.setStatus(JobStatus.PENDING);
      job.setPickedBy(null);
      job.setPickedAt(null);
      return true;
    }
    return false;
  }

  public boolean resetJobToPending(Long jobId) {
    try {
      boolean reset = jobBatchStatusStore.resetRunningJob(jobId, nodeIdentityProvider.getNodeId());
      if (reset) {
        return true;
      }

      log.warnf("Failed to reset job %s - scheduling for retry buffer", jobId);
    } catch (Exception e) {
      log.errorf(e, "Reset to PENDING error for job %s", jobId);
    }

    return false;
  }

  /** Resets all RUNNING jobs owned by this node to PENDING. */
  public int resetRunningJobsForNode() {
    return jobBatchStatusStore.resetRunningJobs(nodeIdentityProvider.getNodeId());
  }
}
