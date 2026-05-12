package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBatchStatusStore;

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
   *
   * <p>The entity update is optimistic. If the surrounding transaction rolls back after this method
   * returns, callers must discard or reload the entity before using its status fields.
   *
   * <p>Transaction attribute: REQUIRED.
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

  /**
   * Resets a RUNNING job owned by this node back to PENDING.
   *
   * <p>Transaction attribute: REQUIRED.
   */
  public boolean resetJobToPending(UUID jobId) {
    try {
      boolean reset = jobBatchStatusStore.resetRunningJob(jobId, nodeIdentityProvider.getNodeId());
      if (reset) {
        return true;
      }

      log.warnf("Failed to reset job %s - scheduling for retry buffer", jobId);
    } catch (Exception e) {
      log.errorf(e, "Reset to PENDING error for job %s", jobId);
      throw new IllegalStateException("Failed to reset job " + jobId + " to PENDING", e);
    }

    return false;
  }

  /**
   * Resets all RUNNING jobs owned by this node to PENDING.
   *
   * <p>Transaction attribute: REQUIRED.
   */
  public int resetRunningJobsForNode() {
    return jobBatchStatusStore.resetRunningJobs(nodeIdentityProvider.getNodeId());
  }
}
