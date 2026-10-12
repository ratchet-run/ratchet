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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
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
 * completion path, including failure callbacks when payload hydration succeeds.
 *
 * @see BatchRecoveryTimer
 */
public class OrphanRecoveryTimer {

  private static final Logger log = Logger.getLogger(OrphanRecoveryTimer.class);
  private static final String LEASE_NAME = "orphanRecovery";

  static final int EXHAUSTED_LIMIT = 100;
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
    if (jobBulkStore == null || nodeStore == null || resourcePermitService == null) {
      throw new IllegalStateException("OrphanRecoveryTimer dependencies are not initialized");
    }

    Instant cutoff = effective().instant().minusSeconds(orphanGraceSeconds);
    OrphanRecovery recovery =
        jobBulkStore.resetOrphanJobsBefore(cutoff, maxCrashRedeliveries, EXHAUSTED_LIMIT);
    int resetJobs = recovery.reset();
    for (ExhaustedOrphan orphan : recovery.exhausted()) {
      failExhausted(orphan);
    }
    List<NodeEntity> staleNodes = nodeStore.findInactiveNodesSince(cutoff);

    int cleanedPermits = 0;
    int deletedNodes = 0;

    if (!staleNodes.isEmpty()) {
      List<String> staleNodeIds = staleNodes.stream().map(NodeEntity::getId).toList();
      cleanedPermits = resourcePermitService.cleanupOrphanedPermits(staleNodeIds);
      deletedNodes = nodeStore.deleteInactiveNodesByIds(staleNodeIds);
    }

    if (resetJobs > 0 || cleanedPermits > 0 || deletedNodes > 0) {
      log.infof(
          "Orphan recovery: reset %s job(s), cleaned %s permit(s), removed %s stale node(s)",
          resetJobs, cleanedPermits, deletedNodes);
    }
  }

  private void failExhausted(ExhaustedOrphan orphan) {
    try {
      JobEntity job;
      boolean hydrated = true;
      try {
        Optional<JobEntity> loaded = jobCrudStore.findById(orphan.jobId());
        if (loaded.isEmpty()) {
          return;
        }
        job = loaded.get();
      } catch (Exception hydrationError) {
        log.warnf(
            hydrationError,
            "Cannot hydrate exhausted orphan %s; failing from metadata",
            orphan.jobId());
        Optional<JobEntity> snapshot = jobBulkStore.findOrphanCompletionSnapshot(orphan.jobId());
        if (snapshot.isEmpty()) {
          return;
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
        return;
      }
      job.setStatus(JobStatus.FAILED);
      if (hydrated) {
        failureCallback.accept(job, failure);
      }
    } catch (Exception failure) {
      log.errorf(failure, "Cannot fail exhausted orphan %s; next scan will retry", orphan.jobId());
    }
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
