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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.internal.StringKeyNormalizer;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.OrphanRecovery;

/**
 * Maintains node liveness and startup recovery for every node identity provider.
 *
 * <p>{@link #init()} registers the node and recovers its own claims. {@link
 * #startHeartbeat(ScheduledExecutorService)} runs the periodic heartbeat on the runtime's scheduled
 * executor. The runtime skips it only when that executor is unavailable. Polling is then off too,
 * so the node holds no claims while its row goes stale.
 */
public class NodeLiveness {

  private static final int MAX_STARTUP_RECOVERY_ATTEMPTS = 3;

  private static final Logger log = Logger.getLogger(NodeLiveness.class);

  /**
   * Heuristic message fragments used to suppress expected heartbeat failures during CDI container
   * shutdown. The class-name check for {@code ContextNotActiveException} (a standard CDI type) is
   * the primary portable guard; these strings catch implementations that wrap or rethrow with
   * implementation-specific messages. The Weld-specific codes ({@code WELD-000229}, {@code
   * WELD-001303}) are intentionally retained as secondary markers because Weld is the dominant EE
   * runtime, but they are never the sole check — other CDI implementations that surface the
   * portable {@code ContextNotActiveException} or the generic "No active contexts" phrase are also
   * handled without any Weld dependency.
   */
  private static final List<String> CONTAINER_SHUTDOWN_ERROR_MARKERS =
      List.of("WELD-000229", "WELD-001303", "after container", "No active contexts for scope type");

  final AtomicBoolean initialized = new AtomicBoolean();
  private final NodeStore nodeStore;
  private final JobBulkStore jobBulkStore;
  private final DynamicHeartbeatCalculator heartbeatCalculator;
  private final long heartbeatIntervalSeconds;
  private final long orphanGraceSeconds;
  private final int maxCrashRedeliveries;
  private final boolean dynamicHeartbeatEnabled;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final Clock clock;
  private final LongUnaryOperator retryDelayJitter;
  private final Object heartbeatLifecycleMonitor = new Object();

  private ScheduledExecutorService heartbeatScheduler;
  private volatile ScheduledFuture<?> heartbeatHandle;
  private volatile String nodeId;

  protected NodeLiveness() {
    this.nodeStore = null;
    this.jobBulkStore = null;
    this.heartbeatCalculator = null;
    this.heartbeatIntervalSeconds = 0;
    this.orphanGraceSeconds = 0;
    this.maxCrashRedeliveries = 0;
    this.dynamicHeartbeatEnabled = false;
    this.nodeIdentityProvider = null;
    this.clock = null;
    this.retryDelayJitter = NodeLiveness::withRetryJitter;
  }

  public NodeLiveness(
      NodeIdentityProvider nodeIdentityProvider,
      NodeStore nodeStore,
      JobBulkStore jobBulkStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      int maxCrashRedeliveries,
      boolean dynamicHeartbeatEnabled,
      Clock clock) {
    this(
        nodeIdentityProvider,
        nodeStore,
        jobBulkStore,
        heartbeatCalculator,
        heartbeatIntervalSeconds,
        orphanGraceSeconds,
        maxCrashRedeliveries,
        dynamicHeartbeatEnabled,
        clock,
        NodeLiveness::withRetryJitter);
  }

  NodeLiveness(
      NodeIdentityProvider nodeIdentityProvider,
      NodeStore nodeStore,
      JobBulkStore jobBulkStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      int maxCrashRedeliveries,
      boolean dynamicHeartbeatEnabled,
      Clock clock,
      LongUnaryOperator retryDelayJitter) {
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.nodeStore = nodeStore;
    this.jobBulkStore = jobBulkStore;
    this.heartbeatCalculator = heartbeatCalculator;
    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    this.orphanGraceSeconds = orphanGraceSeconds;
    this.maxCrashRedeliveries = maxCrashRedeliveries;
    this.dynamicHeartbeatEnabled = dynamicHeartbeatEnabled;
    this.clock = clock;
    this.retryDelayJitter =
        retryDelayJitter != null ? retryDelayJitter : NodeLiveness::withRetryJitter;
  }

  private static boolean isContainerShutdownException(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String className = current.getClass().getName();
      if (className.endsWith("ContextNotActiveException")) {
        return true;
      }

      String message = current.getMessage();
      if (message != null) {
        for (String marker : CONTAINER_SHUTDOWN_ERROR_MARKERS) {
          if (message.contains(marker)) {
            return true;
          }
        }
      }
      current = current.getCause();
    }
    return false;
  }

  /** Resolves the node ID, sends an initial heartbeat, and recovers startup claims. */
  public void init() {
    if (!initialized.compareAndSet(false, true)) {
      log.warn("NodeLiveness already initialized; skipping re-run");
      return;
    }

    String resolvedNodeId =
        StringKeyNormalizer.optional(nodeIdentityProvider.getNodeId(), "node id");
    nodeId = resolvedNodeId;
    log.infof("Scheduler nodeId=%s", resolvedNodeId);

    checkClockSkew();

    nodeStore.upsertHeartbeat(resolvedNodeId, effective().instant());

    // Startup self-recovery: unconditionally reclaim RUNNING jobs owned by THIS nodeId. A node
    // that crashes and restarts inside the steady-state grace window would otherwise leave its
    // own prior RUNNING rows in place until the heartbeat aged out. Unlike the periodic scan this
    // has no page or time limit: once this node heartbeats, no periodic scan sees its claims.
    int scanned = 0;
    int reset = 0;
    int released = 0;
    while (true) {
      OrphanRecovery own =
          recoverAtStartup(
              resolvedNodeId,
              () ->
                  jobBulkStore.resetOrphanJobsForNode(
                      resolvedNodeId, maxCrashRedeliveries, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
      scanned += own.scanned();
      reset += own.reset();
      released += own.released();
      if (own.scanned() < OrphanRecoveryTimer.ORPHAN_PAGE_SIZE) {
        break;
      }
      if (own.reset() + own.released() == 0) {
        // Every row the page read left this node's claims before the recheck, for example to a
        // peer's scan. More may remain. No row can join until this node claims, so read again.
        log.warnf(
            "Startup recovery for node %s read a full page that changed nothing; reading again",
            resolvedNodeId);
      }
    }
    log.infof(
        "Startup recovery for node %s: scanned %s candidate(s), reset %s job(s), released %s"
            + " owner(s)",
        resolvedNodeId, scanned, reset, released);

    // The orphan recovery timer runs this scan again after one interval, so only the node's own
    // reset above must succeed for startup to continue.
    try {
      OrphanRecovery others =
          recoverAtStartup(
              resolvedNodeId,
              () ->
                  jobBulkStore.resetOrphanJobsBefore(
                      effective().instant().minusSeconds(orphanGraceSeconds),
                      maxCrashRedeliveries,
                      OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
      log.infof(
          "Startup orphan recovery: scanned %s candidate(s), reset %s job(s), released %s owner(s);"
              + " exhausted claims deferred by this page are failed by the first periodic scan",
          others.scanned(), others.reset(), others.released());
    } catch (RatchetTransientStoreException e) {
      log.warnf(
          e,
          "Startup orphan recovery failed for node %s; the orphan recovery timer will retry",
          resolvedNodeId);
    }
  }

  /** Starts periodic heartbeats on the runtime's scheduled executor after initialization. */
  public void startHeartbeat(ScheduledExecutorService scheduler) {
    Objects.requireNonNull(scheduler, "scheduler");
    synchronized (heartbeatLifecycleMonitor) {
      if (!initialized.get()) {
        return;
      }
      if (heartbeatScheduler != null) {
        log.warn("NodeLiveness heartbeat already started; skipping re-run");
        return;
      }
      heartbeatScheduler = scheduler;
      scheduleNextHeartbeat();
    }
  }

  /**
   * Retries transient startup recovery failures, propagating failure when attempts are exhausted.
   *
   * <p>Each attempt is a separate store call and transaction, so a failed page has rolled back
   * before its retry, even when a caller transaction is open around {@code init()}.
   */
  private OrphanRecovery recoverAtStartup(String nodeId, Supplier<OrphanRecovery> recovery) {
    for (int attempt = 1; ; attempt++) {
      try {
        return recovery.get();
      } catch (RatchetTransientStoreException e) {
        if (attempt == MAX_STARTUP_RECOVERY_ATTEMPTS) {
          throw e;
        }
        log.warnf(
            e,
            "Transient startup recovery failure for node %s; retrying after attempt %s/%s",
            nodeId,
            attempt,
            MAX_STARTUP_RECOVERY_ATTEMPTS);
      }
    }
  }

  /** Shuts down the heartbeat scheduler. */
  public void shutdown() {
    initialized.set(false);
    synchronized (heartbeatLifecycleMonitor) {
      initialized.set(false);
      ScheduledFuture<?> handle = heartbeatHandle;
      if (handle != null) {
        handle.cancel(true);
      }
      // Wait for any in-flight heartbeat callback to observe initialized=false and exit.
      heartbeatHandle = null;
      heartbeatScheduler = null;
    }
  }

  /**
   * Warns if app-server and DB clocks differ by more than 5 seconds. Skew can cause premature
   * orphan recovery or stale heartbeats.
   */
  private void checkClockSkew() {
    try {
      Instant dbTime = nodeStore.getDatabaseTime();
      Instant appTime = effective().instant();
      long skewSeconds = Math.abs(Duration.between(dbTime, appTime).toSeconds());

      if (skewSeconds > 5) {
        log.warnf(
            "Clock skew: app/db differ by %ss (app=%s, db=%s) — sync clocks via NTP to avoid"
                + " double-execution",
            skewSeconds, appTime, dbTime);
      } else {
        log.debugf("Clock skew check passed: %ss difference", skewSeconds);
      }
    } catch (Exception e) {
      log.warnf(e, "Clock skew check skipped: %s", e.getMessage());
    }
  }

  private void scheduleHeartbeatWithDelay(long delaySeconds) {
    synchronized (heartbeatLifecycleMonitor) {
      if (!initialized.get()) {
        return;
      }

      heartbeatHandle =
          heartbeatScheduler.schedule(
              () -> runHeartbeat("Heartbeat retry failed", delaySeconds, false),
              delaySeconds,
              TimeUnit.SECONDS);
    }
  }

  private void scheduleNextHeartbeat() {
    synchronized (heartbeatLifecycleMonitor) {
      if (!initialized.get()) {
        return;
      }

      if (heartbeatHandle != null && !heartbeatHandle.isDone()) {
        heartbeatHandle.cancel(false);
      }

      long intervalSeconds;
      if (dynamicHeartbeatEnabled) {
        intervalSeconds = heartbeatCalculator.calculateHeartbeatInterval();
      } else {
        intervalSeconds = heartbeatIntervalSeconds;
      }

      heartbeatHandle =
          heartbeatScheduler.schedule(
              () -> runHeartbeat("Heartbeat failed", intervalSeconds, true),
              intervalSeconds,
              TimeUnit.SECONDS);
    }
  }

  private void runHeartbeat(String failureMessage, long failureDelaySeconds, boolean logSuccess) {
    synchronized (heartbeatLifecycleMonitor) {
      if (!initialized.get()) {
        return;
      }

      try {
        nodeStore.upsertHeartbeat(nodeId, effective().instant());
        if (logSuccess) {
          log.debugf("Heartbeat sent for node %s", nodeId);
        }
        if (initialized.get()) {
          scheduleNextHeartbeat();
        }
      } catch (Exception e) {
        if (!initialized.get()) {
          return;
        }
        if (isContainerShutdownException(e)) {
          initialized.set(false);
          log.debugf("Suppressing heartbeat failure during container shutdown: %s", e);
          return;
        }
        log.error(failureMessage, e);
        scheduleHeartbeatWithDelay(retryDelaySeconds(failureDelaySeconds));
      }
    }
  }

  private long retryDelaySeconds(long failureDelaySeconds) {
    long doubled =
        failureDelaySeconds > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : failureDelaySeconds * 2;
    long cappedDelay = Math.min(doubled, orphanGraceSeconds);
    long jitteredDelay = retryDelayJitter.applyAsLong(cappedDelay);
    return Math.max(0, Math.min(jitteredDelay, orphanGraceSeconds));
  }

  private static long withRetryJitter(long cappedDelaySeconds) {
    if (cappedDelaySeconds <= 1) {
      return cappedDelaySeconds;
    }
    long jitterRange = Math.max(1, cappedDelaySeconds / 10);
    long lowerBound = Math.max(1, cappedDelaySeconds - jitterRange);
    long upperBound = cappedDelaySeconds + jitterRange;
    return ThreadLocalRandom.current().nextLong(lowerBound, upperBound + 1);
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
