package run.ratchet.ri.core;

import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStatusStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

/**
 * Manages job state transitions within the scheduler framework, particularly for resetting jobs
 * back to PENDING status for reprocessing.
 *
 * <p>This service is critical for cluster resilience and graceful degradation scenarios:
 *
 * <ul>
 *   <li><b>Node Shutdown:</b> Resets all jobs running on the shutting-down node
 *   <li><b>Job Failure Recovery:</b> Allows failed jobs to be reset for retry
 *   <li><b>Timeout Handling:</b> Enables timed-out jobs to be requeued
 *   <li><b>Manual Intervention:</b> Supports administrative reset operations
 * </ul>
 *
 * <p>All state transitions are performed atomically through the database to ensure consistency in
 * distributed cluster environments. The node ID is used to ensure only the owning node can reset
 * its jobs, preventing race conditions.
 *
 * @see JobStatusStore for the underlying database operations
 * @see NodeIdentityProvider for node identification
 */
@ApplicationScoped
@Transactional
public class JobStateManager {

  private static final Logger log = Logger.getLogger(JobStateManager.class);

  /**
   * Store for job status transition operations.
   *
   * <p>Provides atomic update operations for job state management.
   */
  private final JobStatusStore jobStatusStore;

  /**
   * Provider for the unique identifier of this cluster node.
   *
   * <p>Used to ensure jobs can only be reset by the node that owns them, preventing cross-node
   * interference in distributed environments.
   */
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
   * Attempts to reset a single job's state to PENDING if it is currently owned by this node.
   *
   * <p>This method performs an atomic database update that:
   *
   * <ol>
   *   <li>Verifies the job is in RUNNING status
   *   <li>Verifies the job is assigned to this node (via pickedBy)
   *   <li>Updates status to PENDING and clears assignment fields
   * </ol>
   *
   * <p>On successful reset, the passed-in JobEntity is also updated in-memory to reflect the new
   * state (status=PENDING, pickedBy=null, pickedAt=null). This keeps the in-memory entity
   * consistent with the database state.
   *
   * <p>Thread safety: This method is safe to call concurrently as the database provides atomicity
   * guarantees on the update operation.
   *
   * @param job the JobEntity to reset; must be a valid job currently in RUNNING status on this
   *     node. On success, this object is updated to reflect the reset state.
   * @return {@code true} if the job was successfully reset to PENDING; {@code false} if the reset
   *     failed (wrong status, wrong node, version conflict, or database error)
   */
  public boolean resetJobToPending(JobEntity job) {
    if (resetJobToPending(job.getId())) {
      // Update in-memory entity to match database state
      job.setStatus(JobStatus.PENDING);
      job.setPickedBy(null);
      job.setPickedAt(null);
      return true;
    }
    return false;
  }

  /**
   * Attempts to reset a single job's state to PENDING by job ID.
   *
   * <p>This overload is used when only the job ID is available (e.g., from a DTO), avoiding the
   * need to load the full entity.
   *
   * @param jobId the ID of the job to reset
   * @return {@code true} if the job was successfully reset to PENDING
   */
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

  /**
   * Resets all jobs currently assigned to this node from RUNNING back to PENDING status.
   *
   * <p>This bulk operation is typically called during node shutdown to ensure jobs are not orphaned
   * in RUNNING status when the node goes offline. Without this reset, jobs would remain stuck until
   * manual intervention or timeout-based recovery.
   *
   * <p>The reset operation:
   *
   * <ul>
   *   <li>Finds all jobs with status=RUNNING and pickedBy=thisNodeId
   *   <li>Updates their status to PENDING
   *   <li>Clears the pickedBy and pickedAt fields
   * </ul>
   *
   * <p>Other nodes in the cluster can then pick up these jobs during their normal polling cycles.
   *
   * @return the number of jobs that were successfully reset to PENDING status
   */
  public int resetRunningJobsForNode() {
    return jobStatusStore.resetRunningJobs(nodeIdentityProvider.getNodeId());
  }
}
