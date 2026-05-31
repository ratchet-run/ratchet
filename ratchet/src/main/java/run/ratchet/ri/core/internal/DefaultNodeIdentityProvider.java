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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;

/**
 * Generates a node ID and maintains liveness via periodic heartbeats.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>An explicit value passed to the constructor (typically wired from {@link
 *       run.ratchet.api.RatchetOptions} {@code .node().nodeId(...)}). Recommended for any
 *       deployment where node identity must survive container/host renames or where two nodes might
 *       otherwise hash-collide on hostname+PID.
 *   <li>{@code hostname-PID-<8-char UUID>} — adequate for single-node and small clusters where the
 *       hostname is durable across restarts.
 *   <li>A random UUID — last-ditch fallback when even hostname resolution fails.
 * </ol>
 *
 * <p><b>When to set an explicit node ID:</b> rolling deploys with ephemeral hostnames (Kubernetes
 * pods, container schedulers), multi-tenant deployments where audit trails should remain stable
 * across restarts, and any cluster &gt;~10 nodes where the hostname-PID hash space risks collision.
 *
 * @see NodeIdentityProvider
 */
public class DefaultNodeIdentityProvider implements NodeIdentityProvider {

  private static final Logger log = Logger.getLogger(DefaultNodeIdentityProvider.class);

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
  private final ExecutorProvider executorProvider;
  private final long heartbeatIntervalSeconds;
  private final long orphanGraceSeconds;
  private final boolean dynamicHeartbeatEnabled;
  private final String explicitNodeId;
  private final Clock clock;
  private final LongUnaryOperator retryDelayJitter;
  private final Object heartbeatLifecycleMonitor = new Object();

  private volatile ScheduledFuture<?> heartbeatHandle;
  private volatile String nodeId;

  protected DefaultNodeIdentityProvider() {
    this.nodeStore = null;
    this.jobBulkStore = null;
    this.heartbeatCalculator = null;
    this.executorProvider = null;
    this.heartbeatIntervalSeconds = 0;
    this.orphanGraceSeconds = 0;
    this.dynamicHeartbeatEnabled = false;
    this.explicitNodeId = null;
    this.clock = null;
    this.retryDelayJitter = DefaultNodeIdentityProvider::withRetryJitter;
  }

  public DefaultNodeIdentityProvider(
      NodeStore nodeStore,
      JobBulkStore jobBulkStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      ExecutorProvider executorProvider,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      boolean dynamicHeartbeatEnabled) {
    this(
        nodeStore,
        jobBulkStore,
        heartbeatCalculator,
        executorProvider,
        heartbeatIntervalSeconds,
        orphanGraceSeconds,
        dynamicHeartbeatEnabled,
        null,
        Clock.systemUTC());
  }

  public DefaultNodeIdentityProvider(
      NodeStore nodeStore,
      JobBulkStore jobBulkStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      ExecutorProvider executorProvider,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      boolean dynamicHeartbeatEnabled,
      String explicitNodeId) {
    this(
        nodeStore,
        jobBulkStore,
        heartbeatCalculator,
        executorProvider,
        heartbeatIntervalSeconds,
        orphanGraceSeconds,
        dynamicHeartbeatEnabled,
        explicitNodeId,
        Clock.systemUTC());
  }

  public DefaultNodeIdentityProvider(
      NodeStore nodeStore,
      JobBulkStore jobBulkStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      ExecutorProvider executorProvider,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      boolean dynamicHeartbeatEnabled,
      String explicitNodeId,
      Clock clock) {
    this(
        nodeStore,
        jobBulkStore,
        heartbeatCalculator,
        executorProvider,
        heartbeatIntervalSeconds,
        orphanGraceSeconds,
        dynamicHeartbeatEnabled,
        explicitNodeId,
        clock,
        DefaultNodeIdentityProvider::withRetryJitter);
  }

  DefaultNodeIdentityProvider(
      NodeStore nodeStore,
      JobBulkStore jobBulkStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      ExecutorProvider executorProvider,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      boolean dynamicHeartbeatEnabled,
      String explicitNodeId,
      Clock clock,
      LongUnaryOperator retryDelayJitter) {
    this.nodeStore = nodeStore;
    this.jobBulkStore = jobBulkStore;
    this.heartbeatCalculator = heartbeatCalculator;
    this.executorProvider = executorProvider;
    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    this.orphanGraceSeconds = orphanGraceSeconds;
    this.dynamicHeartbeatEnabled = dynamicHeartbeatEnabled;
    this.explicitNodeId = explicitNodeId;
    this.clock = clock;
    this.retryDelayJitter =
        retryDelayJitter != null ? retryDelayJitter : DefaultNodeIdentityProvider::withRetryJitter;
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

  @Override
  public String getNodeId() {
    String current = nodeId;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (nodeId == null) {
        nodeId = resolveNodeId();
      }
      return nodeId;
    }
  }

  /** Resolves the node ID, sends an initial heartbeat, and schedules periodic heartbeats. */
  public void init() {
    if (!initialized.compareAndSet(false, true)) {
      log.warn("DefaultNodeIdentityProvider already initialized; skipping re-run");
      return;
    }

    String resolvedNodeId = getNodeId();
    log.infof("Scheduler nodeId=%s", resolvedNodeId);

    checkClockSkew();

    nodeStore.upsertHeartbeat(resolvedNodeId, effective().instant());

    // Startup self-recovery: unconditionally reclaim RUNNING jobs owned by THIS nodeId. A node
    // that crashes and restarts inside the steady-state grace window would otherwise leave its
    // own prior RUNNING rows in place until the heartbeat aged out.
    int ownReset = jobBulkStore.resetOrphanJobsForNode(resolvedNodeId);
    if (ownReset > 0) {
      log.infof(
          "Reset %s RUNNING job(s) owned by this node (%s) at startup", ownReset, resolvedNodeId);
    }

    // Also run the normal grace-based sweep to pick up any other nodes' rows that have aged out.
    int reset = jobBulkStore.resetOrphanJobs(Duration.ofSeconds(orphanGraceSeconds));
    if (reset > 0) {
      log.infof("Reset %s orphan RUNNING job(s) at startup", reset);
    }

    scheduleNextHeartbeat();
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
            "Clock skew: app/db differ by %ss (app=%s, db=%s) — sync clocks via NTP to avoid double-execution",
            skewSeconds, appTime, dbTime);
      } else {
        log.debugf("Clock skew check passed: %ss difference", skewSeconds);
      }
    } catch (Exception e) {
      log.warnf(e, "Clock skew check skipped: %s", e.getMessage());
    }
  }

  private String resolveNodeId() {
    if (explicitNodeId != null && !explicitNodeId.isBlank()) {
      return explicitNodeId;
    }
    try {
      String host = resolveHostnameWithTimeout();
      String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
      return host + "-" + pid + "-" + UUID.randomUUID().toString().substring(0, 8);
    } catch (Exception e) {
      log.warnf(e, "Hostname resolution error, using UUID fallback: %s", e.getMessage());
      return UUID.randomUUID().toString();
    }
  }

  private String resolveHostnameWithTimeout() throws Exception {
    if (executorProvider == null || executorProvider.getJobExecutor() == null) {
      return InetAddress.getLocalHost().getHostName();
    }
    Future<String> future =
        executorProvider.getJobExecutor().submit(() -> InetAddress.getLocalHost().getHostName());
    try {
      return future.get(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new TimeoutException("DNS hostname lookup timed out after 5 seconds");
    }
  }

  private void scheduleHeartbeatWithDelay(long delaySeconds) {
    synchronized (heartbeatLifecycleMonitor) {
      if (!initialized.get()) {
        return;
      }

      heartbeatHandle =
          executorProvider
              .getScheduledExecutor()
              .schedule(
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
          executorProvider
              .getScheduledExecutor()
              .schedule(
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
