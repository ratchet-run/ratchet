package run.ratchet.coordinator.infinispan;

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.JobWakeupHint;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Infinispan embedded-cache {@link ClusterCoordinator}: publishes wakeup envelopes as {@code
 * putAsync} entries on a clustered cache and dispatches inbound {@code CacheEntryCreated} events to
 * registered listeners.
 *
 * <p>Adding this module to a deployment activates push-based cross-node wakeups in place of the
 * default {@code NoOpClusterCoordinator}. Activation is via {@link Alternative} + {@link Priority}.
 *
 * <p>Self-suppression is receive-side only — Infinispan has no cluster-side filter that can drop
 * events by source-node metadata. Cluster bandwidth carries every node's broadcast back to its
 * sender; the receive-side filter discards self-broadcasts.
 *
 * <p>{@link #close()} releases the coordinator's listener registration and dispatch executor. The
 * {@link EmbeddedCacheManager} is provider-owned (typically WildFly subsystem-managed) and is never
 * stopped here — doing so would corrupt other applications using the same cache container.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 100)
// Coordinator @Priority order: see PostgresqlListenNotifyCoordinator. Operators MUST pull in
// exactly one coordinator module; distinct priorities only prevent CDI ambiguity errors on a
// transitive double-pull.
public class InfinispanClusterCoordinator implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(InfinispanClusterCoordinator.class);

  private static final int PRE_REGISTRATION_BUFFER_CAPACITY = 256;

  static final String COORDINATOR_KIND = "infinispan";

  @Inject NodeIdentityProvider identityProvider;
  @Inject InfinispanCoordinatorConfig config;
  @Inject @Any Instance<InfinispanCacheManagerProvider> providerInstance;
  @Inject MetricsCollector metrics;

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private final AtomicLong sendSequence = new AtomicLong();
  private final CopyOnWriteArrayList<Consumer<JobWakeupHint>> listeners =
      new CopyOnWriteArrayList<>();
  private final BlockingQueue<NotifyPayload> preRegistrationBuffer =
      new ArrayBlockingQueue<>(PRE_REGISTRATION_BUFFER_CAPACITY);

  private InfinispanCacheLifecycle cacheLifecycle;
  private Cache<String, String> directCache;
  private ExecutorService listenerExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  protected InfinispanClusterCoordinator() {
    // CDI proxy constructor.
  }

  /** Test/non-CDI constructor that takes a pre-resolved cache directly. */
  InfinispanClusterCoordinator(
      NodeIdentityProvider identityProvider,
      InfinispanCoordinatorConfig config,
      Cache<String, String> cache,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.directCache = Objects.requireNonNull(cache, "cache");
    this.metrics = metrics;
  }

  @PostConstruct
  void init() {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    Cache<String, String> cache;
    if (directCache != null) {
      cache = directCache;
    } else {
      InfinispanCacheManagerProvider provider = resolveProvider();
      EmbeddedCacheManager cacheManager = provider.cacheManager();
      cache = cacheManager.getCache(config.effectiveCacheName());
    }
    cacheLifecycle =
        new InfinispanCacheLifecycle(
            cache, config, codec, this::onInboundNotification, this::onParseFailure);
    listenerExecutor = newListenerExecutor();
  }

  @Override
  public void afterStart() {
    if (closed.get()) {
      return;
    }
    if (cacheLifecycle == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    cacheLifecycle.start();
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (closed.get()) {
      return;
    }
    try {
      String key = source.value() + ":" + sendSequence.incrementAndGet();
      String value = codec.encode(NotifyPayload.current(source, priority, executionTarget));
      cacheLifecycle
          .publish(key, value)
          .whenComplete(
              (v, throwable) -> clusterWakeupPublished(throwable == null ? "success" : "failure"));
    } catch (RuntimeException ex) {
      clusterWakeupPublished("failure");
      log.warnf(
          ex,
          "Infinispan coordinator notifyNewWork transport/encode failure: %s — wakeup dropped",
          ex.getMessage());
    }
  }

  @Override
  public void registerWakeupListener(Consumer<JobWakeupHint> listener) {
    Objects.requireNonNull(listener, "listener");
    if (closed.get()) {
      return;
    }
    listeners.add(listener);
    drainPreRegistrationBuffer();
  }

  /**
   * Hook chain entry point — runs during {@code RatchetLifecycle.onShutdown} after pollers and the
   * execution coordinator have stopped. Delegates to {@link #close()}, which is idempotent.
   */
  @Override
  public void afterStop() {
    close();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    InfinispanCacheLifecycle lifecycle = this.cacheLifecycle;
    if (lifecycle != null) {
      lifecycle.close();
    }
    ExecutorService executor = this.listenerExecutor;
    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(config.shutdownGraceMs(), TimeUnit.MILLISECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Dispatch path from the cache listener. Self-suppresses then routes to listeners. */
  void onInboundNotification(NotifyPayload msg) {
    NodeIdentity local;
    try {
      local = new NodeIdentity(identityProvider.getNodeId());
    } catch (RuntimeException e) {
      clusterWakeupReceived("ignored_provider_error");
      return;
    }
    if (msg.node().equals(local)) {
      clusterWakeupReceived("ignored_self");
      return;
    }
    clusterWakeupReceived("delivered");
    if (listeners.isEmpty()) {
      bufferOrDropOldest(msg);
      return;
    }
    dispatchToListeners(msg);
  }

  private void dispatchToListeners(NotifyPayload msg) {
    JobWakeupHint hint = new JobWakeupHint(msg.priority(), msg.node(), msg.executionTarget());
    for (Consumer<JobWakeupHint> listener : listeners) {
      try {
        listenerExecutor.execute(
            () -> {
              try {
                listener.accept(hint);
              } catch (RuntimeException listenerEx) {
                clusterWakeupReceived("listener_failure");
                log.warnf(
                    listenerEx,
                    "Infinispan coordinator listener threw: %s — suppressing per SPI contract",
                    listenerEx.getMessage());
              }
            });
      } catch (RuntimeException submitEx) {
        log.debugf(
            submitEx,
            "Infinispan coordinator could not enqueue listener task: %s",
            submitEx.getMessage());
      }
    }
  }

  // Counts as overflow regardless of the second offer's outcome — the boolean is irrelevant.
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void bufferOrDropOldest(NotifyPayload msg) {
    if (preRegistrationBuffer.offer(msg)) {
      return;
    }
    preRegistrationBuffer.poll();
    preRegistrationBuffer.offer(msg);
    clusterWakeupReceived("pre_registration_overflow");
    log.warn("Infinispan coordinator pre-registration buffer overflowed; oldest wakeup dropped");
  }

  private void drainPreRegistrationBuffer() {
    NotifyPayload msg;
    while ((msg = preRegistrationBuffer.poll()) != null) {
      dispatchToListeners(msg);
    }
  }

  private void onParseFailure() {
    clusterWakeupReceived("parse_failure");
  }

  private void clusterWakeupPublished(String outcome) {
    metrics.clusterWakeupPublished(COORDINATOR_KIND, outcome);
  }

  private void clusterWakeupReceived(String outcome) {
    metrics.clusterWakeupReceived(COORDINATOR_KIND, outcome);
  }

  private InfinispanCacheManagerProvider resolveProvider() {
    Instance<InfinispanCacheManagerProvider> instance = this.providerInstance;
    if (instance == null || instance.isUnsatisfied()) {
      throw new IllegalStateException(
          "No InfinispanCacheManagerProvider available. Provide a @Produces"
              + " InfinispanCacheManagerProvider or use the WildFly subsystem-bound default.");
    }
    if (instance.isAmbiguous()) {
      log.warn(
          "Multiple InfinispanCacheManagerProvider beans visible; first match wins. Use"
              + " @Alternative + @Priority for disambiguation.");
    }
    return instance.get();
  }

  /** Test accessor for the harness — exposes the lifecycle so readiness can be polled. */
  InfinispanCacheLifecycle lifecycle() {
    return cacheLifecycle;
  }

  private ExecutorService newListenerExecutor() {
    ThreadFactory tf =
        new ThreadFactory() {
          private final AtomicLong counter = new AtomicLong();

          @Override
          public Thread newThread(Runnable r) {
            Thread t =
                new Thread(
                    r, "ratchet-coordinator-infinispan-dispatch-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          }
        };
    return Executors.newFixedThreadPool(Math.max(1, config.listenerExecutorThreads()), tf);
  }
}
