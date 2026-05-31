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
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.SingletonLease;

/**
 * Timer that delegates periodic batch recovery to {@link BatchService}.
 *
 * <p>Recovers stuck batches every 15 minutes — batches where all children have completed but the
 * completion flag was never set (due to crash, network partition, or transaction rollback).
 *
 * @see BatchService#recoverStuckBatches()
 */
@ApplicationScoped
public class BatchRecoveryTimer {

  private static final Logger log = Logger.getLogger(BatchRecoveryTimer.class);
  private static final String LEASE_NAME = "batchRecovery";
  private static final Duration LEASE_TTL = Duration.ofMinutes(15);
  private static final long INITIAL_DELAY_MINUTES = 1;
  private static final long PERIOD_MINUTES = 15;

  private final BatchService batchService;
  private final SingletonLeaseService singletonLeaseService;

  private volatile ScheduledFuture<?> handle;

  protected BatchRecoveryTimer() {
    this.batchService = null;
    this.singletonLeaseService = null;
  }

  public BatchRecoveryTimer(BatchService batchService) {
    this(batchService, null);
  }

  @Inject
  public BatchRecoveryTimer(
      BatchService batchService, SingletonLeaseService singletonLeaseService) {
    this.batchService = batchService;
    this.singletonLeaseService = singletonLeaseService;
  }

  public void start(ScheduledExecutorService executor) {
    handle =
        executor.scheduleAtFixedRate(
            this::recoverBatches, INITIAL_DELAY_MINUTES, PERIOD_MINUTES, TimeUnit.MINUTES);
    log.info("Initialized batch recovery timer; first scan in 1min, then every 15min");
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
        Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
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
