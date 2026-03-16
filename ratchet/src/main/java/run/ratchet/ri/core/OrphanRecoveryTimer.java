package run.ratchet.ri.core;

import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Timer that periodically recovers orphaned jobs from crashed nodes.
 *
 * <p>An orphaned job is one stuck in RUNNING status on a node whose heartbeat has gone stale.
 * Without periodic recovery, these jobs would remain stuck until a node restart.
 *
 * <p>Each scan performs three cleanup operations:
 *
 * <ol>
 *   <li>Resets orphaned RUNNING jobs to PENDING so they can be re-claimed
 *   <li>Releases resource permits held by dead nodes
 *   <li>Deletes stale node registrations
 * </ol>
 *
 * @see BatchRecoveryTimer for the similar pattern used for batch recovery
 */
public class OrphanRecoveryTimer {

  private static final Logger log = Logger.getLogger(OrphanRecoveryTimer.class.getName());

  private final JobBulkStore jobBulkStore;
  private final NodeStore nodeStore;
  private final ResourcePermitService resourcePermitService;
  private final long orphanGraceSeconds;

  private volatile ScheduledFuture<?> handle;

  // Required by CDI proxy
  protected OrphanRecoveryTimer() {
    this.jobBulkStore = null;
    this.nodeStore = null;
    this.resourcePermitService = null;
    this.orphanGraceSeconds = 0;
  }

  public OrphanRecoveryTimer(
      JobBulkStore jobBulkStore, NodeStore nodeStore, ResourcePermitService resourcePermitService) {
    this(jobBulkStore, nodeStore, resourcePermitService, 60);
  }

  /**
   * Creates a new OrphanRecoveryTimer with explicit configuration.
   *
   * @param jobBulkStore store for bulk job operations
   * @param nodeStore store for node health operations
   * @param resourcePermitService service for permit cleanup
   * @param orphanGraceSeconds grace period before jobs are considered orphaned
   */
  public OrphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      NodeStore nodeStore,
      ResourcePermitService resourcePermitService,
      long orphanGraceSeconds) {
    this.jobBulkStore = jobBulkStore;
    this.nodeStore = nodeStore;
    this.resourcePermitService = resourcePermitService;
    this.orphanGraceSeconds = orphanGraceSeconds;
  }

  /**
   * Starts the orphan recovery timer on the provided executor.
   *
   * @param executor the scheduled executor to use for periodic execution
   * @param intervalMinutes how often to scan for orphans, in minutes
   */
  public void start(ScheduledExecutorService executor, long intervalMinutes) {
    handle =
        executor.scheduleAtFixedRate(
            this::recoverOrphans, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
    log.info(
        "Initialized orphan recovery timer — scanning every "
            + intervalMinutes
            + "min (grace="
            + orphanGraceSeconds
            + "s)");
  }

  /** Stops the orphan recovery timer. */
  public void stop() {
    if (handle != null) {
      handle.cancel(false);
      handle = null;
    }
  }

  /** Scans for and recovers orphaned jobs. Called periodically by the scheduled executor. */
  void recoverOrphans() {
    try {
      int resetJobs = jobBulkStore.resetOrphanJobs(Duration.ofSeconds(orphanGraceSeconds));

      Instant cutoff = Instant.now().minusSeconds(orphanGraceSeconds);
      List<NodeEntity> staleNodes = nodeStore.findInactiveNodesSince(cutoff);

      int cleanedPermits = 0;
      int deletedNodes = 0;

      if (!staleNodes.isEmpty()) {
        List<String> staleNodeIds = staleNodes.stream().map(NodeEntity::getId).toList();
        cleanedPermits = resourcePermitService.cleanupOrphanedPermits(staleNodeIds);
        deletedNodes = nodeStore.deleteInactiveNodesSince(cutoff);
      }

      if (resetJobs > 0 || cleanedPermits > 0 || deletedNodes > 0) {
        log.info(
            "Orphan recovery: reset "
                + resetJobs
                + " job(s), cleaned "
                + cleanedPermits
                + " permit(s), removed "
                + deletedNodes
                + " stale node(s)");
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Orphan recovery scan failed", e);
    }
  }
}
