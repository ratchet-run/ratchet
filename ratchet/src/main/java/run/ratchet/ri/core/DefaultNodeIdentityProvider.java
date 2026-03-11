package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.NodeStore;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of {@link NodeIdentityProvider} that manages node identity and health
 * monitoring for the distributed job scheduler cluster.
 *
 * <p>This service provides a unique identifier for each scheduler instance and maintains its
 * liveness through periodic heartbeat updates.
 *
 * <p>Node ID generation hierarchy:
 *
 * <ol>
 *   <li>JBoss node name system property (if available)
 *   <li>Hostname + Process ID + Random suffix (standard)
 *   <li>Random UUID (fallback for containerized environments)
 * </ol>
 *
 * @see NodeIdentityProvider
 * @see DynamicHeartbeatCalculator for adaptive interval calculation
 */
public class DefaultNodeIdentityProvider implements NodeIdentityProvider {

  private static final Logger log = Logger.getLogger(DefaultNodeIdentityProvider.class.getName());

  private final AtomicBoolean initialized = new AtomicBoolean();
  private final NodeStore nodeStore;
  private final DynamicHeartbeatCalculator heartbeatCalculator;
  private final ExecutorProvider executorProvider;
  private final long heartbeatIntervalSeconds;
  private final long orphanGraceSeconds;
  private final boolean dynamicHeartbeatEnabled;

  private ScheduledFuture<?> heartbeatHandle;
  private String nodeId;

  // Required by CDI proxy
  protected DefaultNodeIdentityProvider() {
    this.nodeStore = null;
    this.heartbeatCalculator = null;
    this.executorProvider = null;
    this.heartbeatIntervalSeconds = 0;
    this.orphanGraceSeconds = 0;
    this.dynamicHeartbeatEnabled = false;
  }

  /**
   * Creates a new DefaultNodeIdentityProvider.
   *
   * @param nodeStore store for node heartbeat operations
   * @param heartbeatCalculator calculator for dynamic heartbeat intervals
   * @param executorProvider provides scheduled executor for heartbeat tasks
   * @param heartbeatIntervalSeconds base heartbeat frequency in seconds
   * @param orphanGraceSeconds time before jobs are considered orphaned
   * @param dynamicHeartbeatEnabled whether to enable adaptive heartbeat intervals
   */
  public DefaultNodeIdentityProvider(
      NodeStore nodeStore,
      DynamicHeartbeatCalculator heartbeatCalculator,
      ExecutorProvider executorProvider,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      boolean dynamicHeartbeatEnabled) {
    this.nodeStore = nodeStore;
    this.heartbeatCalculator = heartbeatCalculator;
    this.executorProvider = executorProvider;
    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    this.orphanGraceSeconds = orphanGraceSeconds;
    this.dynamicHeartbeatEnabled = dynamicHeartbeatEnabled;
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  /**
   * Initializes the node's operations and configurations during application startup.
   *
   * <p>This method resolves the node identifier, publishes an initial heartbeat, and schedules
   * periodic heartbeat updates.
   */
  public void init() {
    if (!initialized.compareAndSet(false, true)) {
      log.warning("DefaultNodeIdentityProvider already initialized; skipping re-run");
      return;
    }

    nodeId = resolveNodeId();
    log.info("Scheduler nodeId=" + nodeId);

    nodeStore.upsertHeartbeat(nodeId, Instant.now());

    scheduleNextHeartbeat();
  }

  /** Shuts down the heartbeat scheduler. */
  public void shutdown() {
    if (heartbeatHandle != null) {
      heartbeatHandle.cancel(true);
      heartbeatHandle = null;
    }
    initialized.set(false);
  }

  private String resolveNodeId() {
    String jboss = System.getProperty("jboss.node.name");
    if (jboss != null && !jboss.isBlank()) {
      return jboss;
    }
    try {
      String host = resolveHostnameWithTimeout();
      String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
      return host + "-" + pid + "-" + UUID.randomUUID().toString().substring(0, 8);
    } catch (Exception e) {
      log.warning(
          "Failed to resolve hostname for node ID, falling back to UUID: " + e.getMessage());
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
                () -> {
                  try {
                    nodeStore.upsertHeartbeat(nodeId, Instant.now());
                    scheduleNextHeartbeat();
                  } catch (Exception e) {
                    log.log(Level.SEVERE, "Heartbeat retry failed", e);
                    long cappedDelay = Math.min(delaySeconds * 2, orphanGraceSeconds);
                    scheduleHeartbeatWithDelay(cappedDelay);
                  }
                },
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
                () -> {
                  try {
                    nodeStore.upsertHeartbeat(nodeId, Instant.now());
                    log.fine("Heartbeat sent for node " + nodeId);
                    scheduleNextHeartbeat();
                  } catch (Exception e) {
                    log.log(Level.SEVERE, "Heartbeat failed", e);
                    scheduleHeartbeatWithDelay(heartbeatIntervalSeconds);
                  }
                },
                intervalSeconds,
                TimeUnit.SECONDS);
  }
}
