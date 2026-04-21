package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Generates a stable node ID (JBoss node name → hostname+pid+suffix → random UUID) and maintains
 * liveness via periodic heartbeats.
 *
 * @see NodeIdentityProvider
 */
public class DefaultNodeIdentityProvider implements NodeIdentityProvider {

  private static final Logger log = Logger.getLogger(DefaultNodeIdentityProvider.class);
  private static final List<String> CONTAINER_SHUTDOWN_ERROR_MARKERS =
      List.of("WELD-000229", "WELD-001303", "after container", "No active contexts for scope type");

  private final AtomicBoolean initialized = new AtomicBoolean();
  private final NodeStore nodeStore;
  private final JobBulkStore jobBulkStore;
  private final DynamicHeartbeatCalculator heartbeatCalculator;
  private final ExecutorProvider executorProvider;
  private final long heartbeatIntervalSeconds;
  private final long orphanGraceSeconds;
  private final boolean dynamicHeartbeatEnabled;
  private final String explicitNodeId;
  private final Object heartbeatLifecycleMonitor = new Object();

  private ScheduledFuture<?> heartbeatHandle;
  private String nodeId;

  protected DefaultNodeIdentityProvider() {
    this.nodeStore = null;
    this.jobBulkStore = null;
    this.heartbeatCalculator = null;
    this.executorProvider = null;
    this.heartbeatIntervalSeconds = 0;
    this.orphanGraceSeconds = 0;
    this.dynamicHeartbeatEnabled = false;
    this.explicitNodeId = null;
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
        null);
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
    this.nodeStore = nodeStore;
    this.jobBulkStore = jobBulkStore;
    this.heartbeatCalculator = heartbeatCalculator;
    this.executorProvider = executorProvider;
    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    this.orphanGraceSeconds = orphanGraceSeconds;
    this.dynamicHeartbeatEnabled = dynamicHeartbeatEnabled;
    this.explicitNodeId = explicitNodeId;
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
    return nodeId;
  }

  /** Resolves the node ID, sends an initial heartbeat, and schedules periodic heartbeats. */
  public void init() {
    if (!initialized.compareAndSet(false, true)) {
      log.warn("DefaultNodeIdentityProvider already initialized; skipping re-run");
      return;
    }

    nodeId = resolveNodeId();
    log.infof("Scheduler nodeId=%s", nodeId);

    checkClockSkew();

    nodeStore.upsertHeartbeat(nodeId, Instant.now());

    // Startup self-recovery: unconditionally reclaim RUNNING jobs owned by THIS nodeId. A node
    // that crashes and restarts inside the steady-state grace window would otherwise leave its
    // own prior RUNNING rows in place until the heartbeat aged out.
    int ownReset = jobBulkStore.resetOrphanJobsForNode(nodeId);
    if (ownReset > 0) {
      log.infof("Reset %s RUNNING job(s) owned by this node (%s) at startup", ownReset, nodeId);
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
    if (heartbeatHandle != null) {
      heartbeatHandle.cancel(true);
      heartbeatHandle = null;
    }
    synchronized (heartbeatLifecycleMonitor) {
      // Wait for any in-flight heartbeat callback to observe initialized=false and exit.
    }
  }

  /**
   * Warns if app-server and DB clocks differ by more than 5 seconds. Skew can cause premature
   * orphan recovery or stale heartbeats.
   */
  private void checkClockSkew() {
    try {
      Instant dbTime = nodeStore.getDatabaseTime();
      Instant appTime = Instant.now();
      long skewSeconds = Math.abs(Duration.between(dbTime, appTime).toSeconds());

      if (skewSeconds > 5) {
        log.warnf(
            "Clock skew: app/db differ by %ss (app=%s, db=%s) — sync clocks via NTP to avoid double-execution",
            skewSeconds, appTime, dbTime);
      } else {
        log.debugf("Clock skew check passed: %ss difference", skewSeconds);
      }
    } catch (Exception e) {
      log.warnf("Clock skew check skipped: %s", e.getMessage());
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
      log.warnf("Hostname resolution error, using UUID fallback: %s", e.getMessage());
      return UUID.randomUUID().toString();
    }
  }

  private String resolveHostnameWithTimeout() throws Exception {
    ExecutorService dnsExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "node-id-dns-lookup");
              t.setDaemon(true);
              return t;
            });
    try {
      Future<String> future = dnsExecutor.submit(() -> InetAddress.getLocalHost().getHostName());
      return future.get(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new TimeoutException("DNS hostname lookup timed out after 5 seconds");
    } finally {
      dnsExecutor.shutdownNow();
    }
  }

  private void scheduleHeartbeatWithDelay(long delaySeconds) {
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

  private void scheduleNextHeartbeat() {
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
                () -> runHeartbeat("Heartbeat failed", heartbeatIntervalSeconds, true),
                intervalSeconds,
                TimeUnit.SECONDS);
  }

  private void runHeartbeat(String failureMessage, long failureDelaySeconds, boolean logSuccess) {
    synchronized (heartbeatLifecycleMonitor) {
      if (!initialized.get()) {
        return;
      }

      try {
        nodeStore.upsertHeartbeat(nodeId, Instant.now());
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
        long cappedDelay = Math.min(failureDelaySeconds * 2, orphanGraceSeconds);
        scheduleHeartbeatWithDelay(cappedDelay);
      }
    }
  }
}
