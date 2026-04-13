package run.ratchet.ri.core;

import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStatusStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

/** Resets jobs back to PENDING status for reprocessing (shutdown, failure recovery, timeouts). */
@ApplicationScoped
@Transactional
public class JobStateManager {

  private static final Logger log = Logger.getLogger(JobStateManager.class);

  private final JobStatusStore jobStatusStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  // Required by CDI proxy
  protected JobStateManager() {
    this.jobStatusStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public JobStateManager(JobStatusStore jobStatusStore, NodeIdentityProvider nodeIdentityProvider) {
    this.jobStatusStore = jobStatusStore;
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

  /** Overload for when only the job ID is available (e.g., from a DTO). */
  public boolean resetJobToPending(Long jobId) {
    try {
      boolean reset = jobStatusStore.resetRunningJob(jobId, nodeIdentityProvider.getNodeId());
      if (reset) {
        return true;
      }

      log.warnf("Failed to reset job %s - scheduling for retry buffer", jobId);
    } catch (Exception e) {
      log.errorf(e, "Failed to reset job %s to PENDING status", jobId);
    }

    return false;
  }

  /** Resets all RUNNING jobs owned by this node to PENDING. */
  public int resetRunningJobsForNode() {
    return jobStatusStore.resetRunningJobs(nodeIdentityProvider.getNodeId());
  }
}
