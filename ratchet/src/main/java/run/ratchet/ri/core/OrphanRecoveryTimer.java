package run.ratchet.ri.core;

import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

/**
 * Timer that periodically recovers orphaned jobs from crashed nodes.
 *
 * <p>An orphaned job is one stuck in RUNNING status on a node whose heartbeat has gone stale.
 * Without periodic recovery, these jobs would remain stuck until a node restart.
 *
 * <p>Each scan resets orphaned RUNNING jobs to PENDING, releases permits held by dead nodes, and
 * deletes stale node registrations.
 *
 * @see BatchRecoveryTimer
 */
public class OrphanRecoveryTimer {

  private static final Logger log = Logger.getLogger(OrphanRecoveryTimer.class);
  private static final String LEASE_NAME = "orphanRecovery";

  private final JobBulkStore jobBulkStore;
  private final NodeStore nodeStore;
  private final ResourcePermitService resourcePermitService;
  private final SingletonLeaseService singletonLeaseService;
  private final long orphanGraceSeconds;

  private volatile ScheduledFuture<?> handle;
  private volatile Duration leaseTtl = Duration.ofMinutes(2);

  protected OrphanRecoveryTimer() {
    this.jobBulkStore = null;
    this.nodeStore = null;
    this.resourcePermitService = null;
    this.singletonLeaseService = null;
    this.orphanGraceSeconds = 0;
  }

  public OrphanRecoveryTimer(
      JobBulkStore jobBulkStore, NodeStore nodeStore, ResourcePermitService resourcePermitService) {
    this(jobBulkStore, nodeStore, resourcePermitService, null, 60);
  }

  public OrphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      NodeStore nodeStore,
      ResourcePermitService resourcePermitService,
      long orphanGraceSeconds) {
    this(jobBulkStore, nodeStore, resourcePermitService, null, orphanGraceSeconds);
  }

  public OrphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      NodeStore nodeStore,
      ResourcePermitService resourcePermitService,
      SingletonLeaseService singletonLeaseService,
      long orphanGraceSeconds) {
    this.jobBulkStore = jobBulkStore;
    this.nodeStore = nodeStore;
    this.resourcePermitService = resourcePermitService;
    this.singletonLeaseService = singletonLeaseService;
    this.orphanGraceSeconds = orphanGraceSeconds;
  }

  public void start(ScheduledExecutorService executor, long intervalMinutes) {
    leaseTtl = Duration.ofMinutes(Math.max(2, intervalMinutes));
    handle =
        executor.scheduleAtFixedRate(
            this::recoverOrphans, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
    log.infof(
        "Initialized orphan recovery timer — scanning every %smin (grace=%ss)",
        intervalMinutes, orphanGraceSeconds);
  }

  public void stop() {
    if (handle != null) {
      handle.cancel(false);
      handle = null;
    }
  }

  void recoverOrphans() {
    try {
      if (singletonLeaseService != null) {
        Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, leaseTtl);
        if (lease.isEmpty()) {
          log.debug("Orphan recovery skipped - singleton lease held by another node");
          return;
        }

        try (SingletonLease ignored = lease.get()) {
          recoverOrphansWithLease();
        }
        return;
      }

      recoverOrphansWithLease();
    } catch (Exception e) {
      log.error("Orphan recovery scan failed", e);
    }
  }

  private void recoverOrphansWithLease() {
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
      log.infof(
          "Orphan recovery: reset %s job(s), cleaned %s permit(s), removed %s stale node(s)",
          resetJobs, cleanedPermits, deletedNodes);
    }
  }
}
