package run.ratchet.coordinator.hazelcast;

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Hazelcast {@link ITopic}-based {@link ClusterCoordinator}: publishes wakeup envelopes as strings
 * on a cluster-wide topic and dispatches inbound messages to registered listeners.
 *
 * <p>Adding this module to a Payara (or any Hazelcast-equipped) deployment flips push-based wakeups
 * on with no application-side beans.xml edits. Activation is via {@link Alternative} + {@link
 * Priority}.
 *
 * <p>Publishes use {@link ITopic#publishAsync} so a slow broker round-trip under Hazelcast client
 * mode does not block the calling thread; the returned {@link CompletionStage} is observed for
 * failures and metric-logged.
 *
 * <p>Self-suppression is receive-side — Hazelcast has no built-in source-node filter on topics.
 * {@link #close()} removes the message-listener registration and shuts down the dispatch executor;
 * the {@link HazelcastInstance} is provider-owned and never stopped here.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 200)
// Coordinator @Priority order: see PostgresqlListenNotifyCoordinator. Operators MUST pull in
// exactly one coordinator module; distinct priorities only prevent CDI ambiguity errors on a
// transitive double-pull.
public class HazelcastClusterCoordinator implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(HazelcastClusterCoordinator.class);

  private static final int PRE_REGISTRATION_BUFFER_CAPACITY = 256;

  static final String COORDINATOR_KIND = "hazelcast";

  @Inject NodeIdentityProvider identityProvider;
  @Inject HazelcastCoordinatorConfig config;
  @Inject @Any Instance<HazelcastInstanceProvider> providerInstance;
  @Inject MetricsCollector metrics;

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private final CopyOnWriteArrayList<BiConsumer<JobPriority, NodeIdentity>> listeners =
      new CopyOnWriteArrayList<>();
  private final BlockingQueue<NotifyPayload> preRegistrationBuffer =
      new ArrayBlockingQueue<>(PRE_REGISTRATION_BUFFER_CAPACITY);

  private HazelcastInstance directInstance;
  private ITopic<String> topic;
  private UUID listenerRegistrationId;
  private ExecutorService listenerExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  HazelcastClusterCoordinator() {
    // CDI proxy constructor — package-private to keep it out of the public API surface.
  }

  /** Test/non-CDI constructor with a directly-supplied {@link HazelcastInstance}. */
  HazelcastClusterCoordinator(
      NodeIdentityProvider identityProvider,
      HazelcastCoordinatorConfig config,
      HazelcastInstance instance,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.directInstance = Objects.requireNonNull(instance, "instance");
    this.metrics = metrics;
  }

  @PostConstruct
  void init() {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    HazelcastInstance hz;
    if (directInstance != null) {
      hz = directInstance;
    } else {
      HazelcastInstanceProvider provider = resolveProvider();
      hz = provider.hazelcastInstance();
    }
    topic = hz.getTopic(config.effectiveTopicName());
    listenerExecutor = newListenerExecutor();
  }

  @Override
  public void afterStart() {
    if (closed.get()) {
      return;
    }
    if (topic == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    listenerRegistrationId = topic.addMessageListener(new TopicListener());
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (closed.get()) {
      return;
    }
    try {
      String body = codec.encode(NotifyPayload.current(source, priority));
      CompletionStage<Void> stage = topic.publishAsync(body);
      stage.whenCompleteAsync(
          (v, throwable) -> {
            if (throwable == null) {
              clusterWakeupPublished("success");
            } else {
              clusterWakeupPublished("failure");
              log.warnf(
                  "Hazelcast coordinator async publish failed: %s — wakeup dropped",
                  throwable.getMessage());
            }
          },
          listenerExecutor);
    } catch (RuntimeException ex) {
      clusterWakeupPublished("failure");
      log.warnf(
          ex,
          "Hazelcast coordinator notifyNewWork transport/encode failure: %s — wakeup dropped",
          ex.getMessage());
    }
  }

  @Override
  public void registerWakeupListener(BiConsumer<JobPriority, NodeIdentity> listener) {
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
    UUID regId = this.listenerRegistrationId;
    if (regId != null && topic != null) {
      try {
        topic.removeMessageListener(regId);
      } catch (RuntimeException ignored) {
        // best-effort; Hazelcast may already be stopped (provider-driven)
      }
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

  /** Dispatch path from the Hazelcast topic listener thread. */
  void onTopicMessage(String body) {
    NotifyPayload payload;
    try {
      if (body != null && body.length() > config.maxInboundPayloadChars()) {
        clusterWakeupReceived("parse_failure");
        log.warnf(
            "Hazelcast coordinator rejected oversized inbound payload (%d chars > cap %d)",
            body.length(), config.maxInboundPayloadChars());
        return;
      }
      payload = codec.decode(body);
    } catch (RuntimeException parseEx) {
      clusterWakeupReceived("parse_failure");
      log.debugf("Hazelcast coordinator dropped malformed payload: %s", parseEx.getMessage());
      return;
    }
    NodeIdentity local;
    try {
      local = new NodeIdentity(identityProvider.getNodeId());
    } catch (RuntimeException e) {
      clusterWakeupReceived("ignored_provider_error");
      return;
    }
    if (payload.node().equals(local)) {
      clusterWakeupReceived("ignored_self");
      return;
    }
    clusterWakeupReceived("delivered");
    if (listeners.isEmpty()) {
      bufferOrDropOldest(payload);
      return;
    }
    dispatchToListeners(payload);
  }

  private void dispatchToListeners(NotifyPayload msg) {
    for (BiConsumer<JobPriority, NodeIdentity> listener : listeners) {
      try {
        listenerExecutor.execute(
            () -> {
              try {
                listener.accept(msg.priority(), msg.node());
              } catch (RuntimeException listenerEx) {
                clusterWakeupReceived("listener_failure");
                log.warnf(
                    listenerEx,
                    "Hazelcast coordinator listener threw: %s — suppressing per SPI contract",
                    listenerEx.getMessage());
              }
            });
      } catch (RuntimeException submitEx) {
        log.debugf(
            submitEx,
            "Hazelcast coordinator could not enqueue listener task: %s",
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
    log.warn("Hazelcast coordinator pre-registration buffer overflowed; oldest wakeup dropped");
  }

  private void drainPreRegistrationBuffer() {
    NotifyPayload msg;
    while ((msg = preRegistrationBuffer.poll()) != null) {
      dispatchToListeners(msg);
    }
  }

  private void clusterWakeupPublished(String outcome) {
    metrics.clusterWakeupPublished(COORDINATOR_KIND, outcome);
  }

  private void clusterWakeupReceived(String outcome) {
    metrics.clusterWakeupReceived(COORDINATOR_KIND, outcome);
  }

  private HazelcastInstanceProvider resolveProvider() {
    Instance<HazelcastInstanceProvider> instance = this.providerInstance;
    if (instance == null || instance.isUnsatisfied()) {
      throw new IllegalStateException(
          "No HazelcastInstanceProvider available. Provide a @Produces"
              + " HazelcastInstanceProvider or use the Payara JNDI-bound default.");
    }
    if (instance.isAmbiguous()) {
      log.warn(
          "Multiple HazelcastInstanceProvider beans visible; first match wins. Use"
              + " @Alternative + @Priority for disambiguation.");
    }
    return instance.get();
  }

  private ExecutorService newListenerExecutor() {
    ThreadFactory tf =
        new ThreadFactory() {
          private final AtomicLong counter = new AtomicLong();

          @Override
          public Thread newThread(Runnable r) {
            Thread t =
                new Thread(
                    r, "ratchet-coordinator-hazelcast-dispatch-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          }
        };
    return Executors.newFixedThreadPool(Math.max(1, config.listenerExecutorThreads()), tf);
  }

  /**
   * Hazelcast topic listener — adapts the {@link Message} dispatch path to {@link #onTopicMessage}.
   */
  private final class TopicListener implements MessageListener<String> {
    @Override
    public void onMessage(Message<String> message) {
      try {
        onTopicMessage(message.getMessageObject());
      } catch (RuntimeException ignored) {
        // Already metric-logged inside onTopicMessage; swallow so Hazelcast does not tear the
        // listener registration down on an unhandled exception.
      }
    }
  }
}
