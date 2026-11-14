/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core.internal;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.ExhaustedOrphan;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.OrphanRecovery;

/**
 * Timer that periodically recovers orphaned jobs from crashed nodes.
 *
 * <p>An orphaned job is one stuck in RUNNING status on a node whose heartbeat has gone stale.
 * Without periodic recovery, these jobs would remain stuck until a node restart.
 *
 * <p>Each scan charges a separate crash budget. Exhausted claims are failed through the fenced
 * completion path, including failure callbacks when payload hydration succeeds. Each page's
 * exhausted claims are tried in a random order.
 *
 * <p>A node killed during its own scan leaves the lease held. Other nodes release it once the
 * holder's heartbeat is older than the orphan grace, instead of waiting for the lease TTL.
 *
 * @see BatchRecoveryTimer
 */
public class OrphanRecoveryTimer {

  private static final Logger log = Logger.getLogger(OrphanRecoveryTimer.class);
  private static final int MAX_STARTUP_CLEANUP_ATTEMPTS = 3;
  private static final String LEASE_NAME = "orphanRecovery";

  static final int ORPHAN_PAGE_SIZE = 500;
  static final int MAX_ORPHAN_PAGES_PER_SCAN = 20;
  static final Duration ORPHAN_SCAN_BUDGET = Duration.ofSeconds(10);
  static final int STALE_EXECUTION_LIMIT = 500;
  private final JobBulkStore jobBulkStore;
  private final JobCrudStore jobCrudStore;
  private final PostExecutionHandler lifecycleFacade;
  private final BiConsumer<JobEntity, Throwable> failureCallback;
  private final ErrorSanitizer errorSanitizer;
  private final int maxCrashRedeliveries;
  private final NodeStore nodeStore;
  private final ResourcePermitService resourcePermitService;
  private final SingletonLeaseService singletonLeaseService;
  private final long orphanGraceSeconds;
  private final Clock clock;
  private final Random random;
  private final Object scanMonitor = new Object();

  private volatile ScheduledFuture<?> handle;
  private final Duration leaseTtl;

  protected OrphanRecoveryTimer() {
    this.jobBulkStore = null;
    this.jobCrudStore = null;
    this.lifecycleFacade = null;
    this.failureCallback = null;
    this.errorSanitizer = null;
    this.maxCrashRedeliveries = 0;
    this.nodeStore = null;
    this.resourcePermitService = null;
    this.singletonLeaseService = null;
    this.orphanGraceSeconds = 0;
    this.clock = null;
    this.random = null;
    this.leaseTtl = null;
  }

  public OrphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      JobCrudStore jobCrudStore,
      NodeStore nodeStore,
      ResourcePermitService resourcePermitService,
      SingletonLeaseService singletonLeaseService,
      PostExecutionHandler lifecycleFacade,
      BiConsumer<JobEntity, Throwable> failureCallback,
      ErrorSanitizer errorSanitizer,
      int maxCrashRedeliveries,
      long orphanGraceSeconds,
      long leaseTtlSeconds,
      Clock clock) {
    this(
        jobBulkStore,
        jobCrudStore,
        nodeStore,
        resourcePermitService,
        singletonLeaseService,
        lifecycleFacade,
        failureCallback,
        errorSanitizer,
        maxCrashRedeliveries,
        orphanGraceSeconds,
        leaseTtlSeconds,
        clock,
        new Random());
  }

  OrphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      JobCrudStore jobCrudStore,
      NodeStore nodeStore,
      ResourcePermitService resourcePermitService,
      SingletonLeaseService singletonLeaseService,
      PostExecutionHandler lifecycleFacade,
      BiConsumer<JobEntity, Throwable> failureCallback,
      ErrorSanitizer errorSanitizer,
      int maxCrashRedeliveries,
      long orphanGraceSeconds,
      long leaseTtlSeconds,
      Clock clock,
      Random random) {
    this.jobBulkStore = Objects.requireNonNull(jobBulkStore, "jobBulkStore must not be null");
    this.jobCrudStore = Objects.requireNonNull(jobCrudStore, "jobCrudStore must not be null");
    this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore must not be null");
    this.resourcePermitService =
        Objects.requireNonNull(resourcePermitService, "resourcePermitService must not be null");
    this.lifecycleFacade =
        Objects.requireNonNull(lifecycleFacade, "lifecycleFacade must not be null");
    this.failureCallback =
        Objects.requireNonNull(failureCallback, "failureCallback must not be null");
    this.errorSanitizer = Objects.requireNonNull(errorSanitizer, "errorSanitizer must not be null");
    this.maxCrashRedeliveries = maxCrashRedeliveries;
    this.singletonLeaseService = singletonLeaseService;
    this.orphanGraceSeconds = orphanGraceSeconds;
    this.leaseTtl = Duration.ofSeconds(leaseTtlSeconds);
    this.clock = clock;
    this.random = Objects.requireNonNull(random, "random must not be null");
  }

  /** Runs once at start-up before the node claims work. Each attempt is its own store call. */
  public int cleanupOwnPermitsAtStartup(String nodeId) {
    if (resourcePermitService == null) {
      throw new IllegalStateException("OrphanRecoveryTimer dependencies are not initialized");
    }
    for (int attempt = 1; ; attempt++) {
      try {
        int deleted = resourcePermitService.cleanupOrphanedPermits(List.of(nodeId));
        if (deleted > 0) {
          log.infof("Deleted %s permit(s) left under node %s by an earlier run", deleted, nodeId);
        }
        return deleted;
      } catch (RatchetTransientStoreException e) {
        log.warnf(
            e,
            "Transient startup permit cleanup failure for node %s on attempt %s/%s",
            nodeId,
            attempt,
            MAX_STARTUP_CLEANUP_ATTEMPTS);
        if (attempt == MAX_STARTUP_CLEANUP_ATTEMPTS) {
          throw e;
        }
      }
    }
  }

  public synchronized void start(ScheduledExecutorService executor, long intervalSeconds) {
    if (handle != null) {
      handle.cancel(false);
    }
    handle =
        executor.scheduleAtFixedRate(
            this::recoverNow, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    log.infof(
        "Initialized orphan recovery timer — scanning every %ss (grace=%ss)",
        intervalSeconds, orphanGraceSeconds);
  }

  public synchronized void stop() {
    ScheduledFuture<?> current = handle;
    handle = null;
    if (current != null) {
      current.cancel(false);
    }
  }

  /**
   * Runs one recovery scan immediately through the same path used by the scheduled timer.
   *
   * @apiNote Internal orchestration seam for deterministic runtime diagnostics and integration
   *     tests. Applications must not invoke this method.
   */
  public void recoverNow() {
    synchronized (scanMonitor) {
      try {
        if (singletonLeaseService != null) {
          Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, leaseTtl);
          if (lease.isEmpty()) {
            List<String> releasedNodeIds = releaseLeaseHeldByStaleNodes();
            if (!releasedNodeIds.isEmpty()) {
              lease = singletonLeaseService.tryAcquire(LEASE_NAME, leaseTtl);
              if (lease.isPresent()) {
                log.infof(
                    "Orphan recovery lease acquired after releasing holds by stale node(s) %s",
                    releasedNodeIds);
              }
            }
          }
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
      } catch (RatchetTransientStoreException e) {
        log.warn("Orphan recovery scan hit a transient store failure; the next scan will retry", e);
      } catch (Exception e) {
        log.error("Orphan recovery scan failed", e);
      }
    }
  }

  private List<String> releaseLeaseHeldByStaleNodes() {
    Instant cutoff = effective().instant().minusSeconds(orphanGraceSeconds);
    List<String> releasedNodeIds = new ArrayList<>();
    for (NodeEntity staleNode : nodeStore.findInactiveNodesSince(cutoff)) {
      String nodeId = staleNode.getId();
      // Re-read to avoid releasing a restarted node's lease or a row another scan removed.
      Optional<NodeEntity> current = nodeStore.findNodeById(nodeId);
      if (current.isPresent()
          && current.get().getLastHeartbeat() != null
          && current.get().getLastHeartbeat().isBefore(cutoff)
          && singletonLeaseService.releaseHeldBy(LEASE_NAME, nodeId)) {
        releasedNodeIds.add(nodeId);
      }
    }
    return releasedNodeIds;
  }

  private void recoverOrphansWithLease() {
    if (jobBulkStore == null || nodeStore == null || resourcePermitService == null) {
      throw new IllegalStateException("OrphanRecoveryTimer dependencies are not initialized");
    }

    Instant now = effective().instant();
    Instant cutoff = now.minusSeconds(orphanGraceSeconds);
    Instant deadline = now.plus(ORPHAN_SCAN_BUDGET);
    try {
      recoverOrphans(cutoff, deadline);
    } finally {
      try {
        resourcePermitService.sweepWaiters();
      } catch (RuntimeException e) {
        log.warn("Resource waiter sweep failed; the next scan will retry", e);
      }
      // Runs after a failed recovery step too; it only touches execution rows.
      closeStaleExecutions(cutoff);
    }
  }

  private void recoverOrphans(Instant cutoff, Instant deadline) {
    int resetJobs = 0;
    int failedJobs = 0;
    int attemptedFailures = 0;
    int pages = 0;
    String stopReason = null;
    scan:
    for (int page = 0; page < MAX_ORPHAN_PAGES_PER_SCAN; page++) {
      if (page > 0 && effective().instant().isAfter(deadline)) {
        stopReason = "budget";
        break;
      }
      OrphanRecovery recovery =
          jobBulkStore.resetOrphanJobsBefore(cutoff, maxCrashRedeliveries, ORPHAN_PAGE_SIZE);
      pages++;
      resetJobs += recovery.reset();
      // A page defers all its exhausted claims with one timestamp, so they return together in the
      // same order. A random order stops a slow failure at the head from using the budget on
      // every scan while the claims behind it are never tried.
      List<ExhaustedOrphan> exhausted = new ArrayList<>(recovery.exhausted());
      Collections.shuffle(exhausted, random);
      for (ExhaustedOrphan orphan : exhausted) {
        if (attemptedFailures > 0 && effective().instant().isAfter(deadline)) {
          stopReason = "budget";
          break scan;
        }
        if (failExhausted(orphan)) {
          failedJobs++;
        }
        attemptedFailures++;
      }
      if (recovery.scanned() < ORPHAN_PAGE_SIZE
          || recovery.reset() + recovery.exhausted().size() == 0) {
        break;
      }
      if (pages == MAX_ORPHAN_PAGES_PER_SCAN) {
        stopReason = "page cap";
      }
    }
    if (stopReason != null) {
      log.infof(
          "Orphan recovery stopped at %s: read %s page(s), reset %s row(s), failed %s exhausted"
              + " row(s); more orphans may remain. Deferred rows return after one grace period;"
              + " the next scan continues",
          stopReason, pages, resetJobs, failedJobs);
    }
    List<NodeEntity> staleNodes = nodeStore.findInactiveNodesSince(cutoff);

    int cleanedPermits = 0;
    int deletedNodes = 0;

    if (!staleNodes.isEmpty()) {
      List<String> staleNodeIds = staleNodes.stream().map(NodeEntity::getId).toList();
      cleanedPermits = resourcePermitService.cleanupOrphanedPermits(staleNodeIds);
      deletedNodes = nodeStore.deleteInactiveNodesByIds(staleNodeIds);
    }

    if (resetJobs > 0 || failedJobs > 0 || cleanedPermits > 0 || deletedNodes > 0) {
      log.infof(
          "Orphan recovery: reset %s job(s), failed %s exhausted job(s), cleaned %s permit(s),"
              + " removed %s stale node(s)",
          resetJobs, failedJobs, cleanedPermits, deletedNodes);
    }
  }

  private void closeStaleExecutions(Instant cutoff) {
    try {
      int closed = jobBulkStore.closeStaleExecutions(cutoff, STALE_EXECUTION_LIMIT);
      if (closed > 0) {
        log.infof("Orphan recovery: closed %s stale execution(s)", closed);
      }
    } catch (RuntimeException e) {
      log.warn("Stale execution cleanup failed", e);
    }
  }

  private boolean failExhausted(ExhaustedOrphan orphan) {
    boolean completed = false;
    try {
      JobEntity job;
      boolean hydrated = true;
      try {
        Optional<JobEntity> loaded = jobCrudStore.findById(orphan.jobId());
        if (loaded.isEmpty()) {
          return false;
        }
        job = loaded.get();
      } catch (Exception hydrationError) {
        log.warnf(
            hydrationError,
            "Cannot hydrate exhausted orphan %s; failing from metadata",
            orphan.jobId());
        Optional<JobEntity> snapshot = jobBulkStore.findOrphanCompletionSnapshot(orphan.jobId());
        if (snapshot.isEmpty()) {
          return false;
        }
        job = snapshot.get();
        hydrated = false;
      }
      job.setClaimSeq(orphan.claimSeq());
      IllegalStateException failure =
          new IllegalStateException(
              "Node "
                  + (orphan.pickedBy() == null ? "unknown" : orphan.pickedBy())
                  + " stopped while running this job; crash redelivery limit ("
                  + maxCrashRedeliveries
                  + ") reached");
      String error;
      try {
        error = errorSanitizer.sanitize(failure);
      } catch (Throwable sanitizerError) {
        log.warnf(sanitizerError, "Cannot sanitize crash failure for %s", orphan.jobId());
        error = failure.getClass().getName();
      }
      job.setLastError(error == null ? failure.getClass().getName() : error);
      if (!lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)) {
        log.warnf(
            "Rejected orphan failure for job %s, stale claimSeq %s, node %s",
            orphan.jobId(), orphan.claimSeq(), orphan.pickedBy());
        return false;
      }
      completed = true;
      job.setStatus(JobStatus.FAILED);
      if (hydrated) {
        failureCallback.accept(job, failure);
      }
    } catch (Exception failure) {
      log.errorf(failure, "Cannot fail exhausted orphan %s; next scan will retry", orphan.jobId());
    }
    return completed;
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
