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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.SingletonLease;

/**
 * Timer that delegates periodic batch recovery to {@link BatchService}.
 *
 * <p>Recovers stuck batches on the configured cadence — batches where all children have completed
 * but the completion flag was never set (due to crash, network partition, or transaction rollback).
 *
 * @see BatchService#recoverStuckBatches()
 */
@ApplicationScoped
public class BatchRecoveryTimer {

  private static final Logger log = Logger.getLogger(BatchRecoveryTimer.class);
  private static final String LEASE_NAME = "batchRecovery";
  private final Duration leaseTtl;
  private final long intervalSeconds;

  private final BatchService batchService;
  private final SingletonLeaseService singletonLeaseService;

  private volatile ScheduledFuture<?> handle;

  protected BatchRecoveryTimer() {
    this.batchService = null;
    this.singletonLeaseService = null;
    this.leaseTtl = null;
    this.intervalSeconds = 0;
  }

  @Inject
  public BatchRecoveryTimer(
      BatchService batchService,
      SingletonLeaseService singletonLeaseService,
      RatchetOptions options) {
    this(
        batchService,
        singletonLeaseService,
        options.maintenance().batchRecoveryIntervalSeconds(),
        options.maintenance().batchRecoveryLeaseTtlSeconds());
  }

  public BatchRecoveryTimer(
      BatchService batchService,
      SingletonLeaseService singletonLeaseService,
      long intervalSeconds,
      long leaseTtlSeconds) {
    this.batchService = batchService;
    this.singletonLeaseService = singletonLeaseService;
    this.intervalSeconds = intervalSeconds;
    this.leaseTtl = Duration.ofSeconds(leaseTtlSeconds);
  }

  public void start(ScheduledExecutorService executor) {
    handle =
        executor.scheduleAtFixedRate(
            this::recoverBatches, Math.min(60, intervalSeconds), intervalSeconds, TimeUnit.SECONDS);
    log.infof(
        "Initialized batch recovery timer; first scan in %ss, then every %ss",
        Math.min(60, intervalSeconds), intervalSeconds);
  }

  public void stop() {
    if (handle != null) {
      handle.cancel(false);
      handle = null;
    }
  }

  void recoverBatches() {
    try {
      if (singletonLeaseService != null) {
        Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, leaseTtl);
        if (lease.isEmpty()) {
          log.debug("Batch recovery skipped - singleton lease held by another node");
          return;
        }

        try (SingletonLease ignored = lease.get()) {
          recoverBatchesWithLease();
        }
        return;
      }

      recoverBatchesWithLease();
    } catch (Exception e) {
      log.error("Batch recovery scan failed", e);
    }
  }

  private void recoverBatchesWithLease() {
    int recovered = batchService.recoverStuckBatches();
    if (recovered > 0) {
      log.infof("Batch recovery timer recovered %s stuck batch(es)", recovered);
    }
  }
}
