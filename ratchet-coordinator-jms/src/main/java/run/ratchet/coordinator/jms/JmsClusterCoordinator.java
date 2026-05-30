package run.ratchet.coordinator.jms;

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
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
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.DecodeException;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.JobWakeupHint;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Jakarta Messaging {@link ClusterCoordinator}: publishes wakeup envelopes as {@link TextMessage}s
 * on a single shared topic and dispatches inbound messages to registered listeners.
 *
 * <p>Adding this module to a Jakarta EE deployment activates push-based cross-node wakeups in place
 * of the default {@code NoOpClusterCoordinator}. Activation is via {@link Alternative} + {@link
 * Priority} — per CDI 4.0 §4.1.1 a {@code @Priority}-annotated alternative is selected globally
 * across all archives, so consumers do not need to edit application-side {@code beans.xml}.
 *
 * <p>Self-suppression is two-layered:
 *
 * <ol>
 *   <li>Broker-side: a JMS selector {@code node <> '<localId>'} is installed on the consumer when
 *       {@link JmsCoordinatorConfig#brokerSideSelfFilter()} is true (default). Saves bandwidth on
 *       brokers that implement selector filtering server-side.
 *   <li>Receive-side: every inbound envelope is compared to the local {@link NodeIdentity}; matches
 *       are dropped and counted as {@code ignored_self}. Always-on defense-in-depth that catches
 *       brokers with buggy selector implementations.
 * </ol>
 *
 * <p>{@link #close()} releases only resources this coordinator allocated: the {@link
 * JmsConnectionLifecycle}-owned context (which the JMS spec says transitively closes its producer
 * and consumer) and the listener executor. The {@link jakarta.jms.ConnectionFactory} and {@link
 * jakarta.jms.Topic} are provider-owned and never closed here.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 300)
// Coordinator @Priority order: see PostgresqlListenNotifyCoordinator. Operators MUST pull in
// exactly one coordinator module; distinct priorities only prevent CDI ambiguity errors on a
// transitive double-pull.
public class JmsClusterCoordinator implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(JmsClusterCoordinator.class);

  private static final int PRE_REGISTRATION_BUFFER_CAPACITY = 256;

  static final String COORDINATOR_KIND = "jms";

  @Inject NodeIdentityProvider identityProvider;

  /**
   * Resolved lazily in {@link #init()}. The config record has a {@code defaults()} factory but is
   * not a managed bean, so it is injected as an {@link Instance} with a defaults() fallback; a
   * direct {@code @Inject JmsCoordinatorConfig} would be an unsatisfied dependency that fails
   * deployment validation out of the box.
   */
  @Inject Instance<JmsCoordinatorConfig> configInstance;

  @Inject @Any Instance<JmsConnectionFactoryProvider> providerInstance;
  @Inject MetricsCollector metrics;

  private JmsCoordinatorConfig config;

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private final CopyOnWriteArrayList<Consumer<JobWakeupHint>> listeners =
      new CopyOnWriteArrayList<>();
  private final BlockingQueue<NotifyPayload> preRegistrationBuffer =
      new ArrayBlockingQueue<>(PRE_REGISTRATION_BUFFER_CAPACITY);

  private JmsConnectionLifecycle connectionLifecycle;
  private ConnectionFactory directConnectionFactory;
  private Topic topic;
  private NodeIdentity localIdentity;
  private ExecutorService listenerExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  protected JmsClusterCoordinator() {
    // CDI proxy constructor.
  }

  /**
   * Test/non-CDI constructor. Bypasses CDI provider resolution by accepting a fully-formed {@link
   * JmsConnectionLifecycle}. Callers must still invoke {@link #init()} before any SPI method.
   */
  JmsClusterCoordinator(
      NodeIdentityProvider identityProvider,
      JmsCoordinatorConfig config,
      JmsConnectionLifecycle lifecycle,
      Topic topic,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.connectionLifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.metrics = metrics;
  }

  /**
   * Test/non-CDI constructor that defers lifecycle construction to {@link #init()} so the lifecycle
   * can be wired with the coordinator's inbound handler ({@code this::onJmsMessage}) — the same
   * path the CDI flow uses. This is the constructor the TCK harness uses.
   */
  JmsClusterCoordinator(
      NodeIdentityProvider identityProvider,
      JmsCoordinatorConfig config,
      ConnectionFactory connectionFactory,
      Topic topic,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.directConnectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.metrics = metrics;
  }

  @PostConstruct
  void init() {
    if (config == null) {
      config =
          configInstance != null && configInstance.isResolvable()
              ? configInstance.get()
              : JmsCoordinatorConfig.defaults();
    }
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    if (connectionLifecycle == null) {
      ConnectionFactory cf;
      if (directConnectionFactory != null) {
        cf = directConnectionFactory;
      } else {
        JmsConnectionFactoryProvider provider = resolveProvider();
        cf = provider.connectionFactory();
        if (topic == null) {
          this.topic = provider.topic();
        }
      }
      connectionLifecycle =
          new JmsConnectionLifecycle(
              cf, topic, config, this::onJmsMessage, this::onConnectionTransportFailure);
    }
    listenerExecutor = newListenerExecutor();
    localIdentity = new NodeIdentity(identityProvider.getNodeId());
  }

  @Override
  public void afterStart() {
    if (closed.get()) {
      return;
    }
    if (connectionLifecycle == null || localIdentity == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    connectionLifecycle.start(localIdentity);
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (closed.get()) {
      return;
    }
    try {
      String body = codec.encode(NotifyPayload.current(source, priority, executionTarget));
      if (!connectionLifecycle.sendTextMessage(body, source.value(), priority.name())) {
        // Reconnect is in flight or initial connect failed — degrade to no-op and metric.
        clusterWakeupPublished("failure");
        return;
      }
      clusterWakeupPublished("success");
    } catch (JMSException jmsEx) {
      // TextMessage.setStringProperty declares JMSException (checked); provider returns it for
      // misuse like a closed session or invalid property type. Treat as transport failure and
      // trigger a reconnect so subsequent calls can recover.
      clusterWakeupPublished("failure");
      log.warnf(
          "JMS coordinator notifyNewWork checked-JMS failure: %s — wakeup dropped",
          jmsEx.getMessage());
      connectionLifecycle.triggerReconnect();
    } catch (JMSRuntimeException jmsRuntimeEx) {
      // Simplified API (jakarta.jms 3.x) raises JMSRuntimeException for transport-side failures
      // such as a dropped connection during send.
      clusterWakeupPublished("failure");
      log.warnf(
          "JMS coordinator notifyNewWork transport failure: %s — wakeup dropped",
          jmsRuntimeEx.getMessage());
      connectionLifecycle.triggerReconnect();
    } catch (RuntimeException runtimeEx) {
      // Defense-in-depth for runtime exceptions outside the JMS hierarchy: JSON-P providers
      // raise JsonException on encode, and vendor extensions may raise their own runtime
      // subclasses. SPI contract is "notifyNewWork never throws"; metric and move on without
      // triggering a reconnect because the fault is encode-side, not transport-side.
      clusterWakeupPublished("failure");
      log.warnf(
          runtimeEx,
          "JMS coordinator notifyNewWork encode/dispatch failure: %s — wakeup dropped",
          runtimeEx.getMessage());
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
    JmsConnectionLifecycle lifecycle = this.connectionLifecycle;
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

  /** Dispatch path from the JMS provider's listener thread. */
  void onJmsMessage(Message message) {
    NotifyPayload payload;
    try {
      if (!(message instanceof TextMessage tm)) {
        clusterWakeupReceived("parse_failure");
        return;
      }
      String body = tm.getText();
      if (body != null && body.length() > config.maxInboundPayloadChars()) {
        // Hard cap on listener-thread allocation: a hostile or buggy producer can otherwise
        // attach a multi-MB body that the codec would happily decode into memory.
        clusterWakeupReceived("parse_failure");
        log.warnf(
            "JMS coordinator rejected oversized inbound payload (%d chars > cap %d)",
            body.length(), config.maxInboundPayloadChars());
        return;
      }
      payload = codec.decode(body);
    } catch (DecodeException ex) {
      clusterWakeupReceived("parse_failure");
      log.debugf("JMS coordinator dropped malformed payload: %s", ex.getMessage());
      return;
    } catch (Exception ex) {
      clusterWakeupReceived("transport_failure");
      log.warnf("JMS coordinator dropped inbound message due to JMS error: %s", ex.getMessage());
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
                    "JMS coordinator listener threw: %s — suppressing per SPI contract",
                    listenerEx.getMessage());
              }
            });
      } catch (RuntimeException submitEx) {
        // Executor refused — log only; the local poller floor is unaffected.
        log.debugf(
            submitEx, "JMS coordinator could not enqueue listener task: %s", submitEx.getMessage());
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
    log.warn("JMS coordinator pre-registration buffer overflowed; oldest wakeup dropped");
  }

  private void drainPreRegistrationBuffer() {
    NotifyPayload msg;
    while ((msg = preRegistrationBuffer.poll()) != null) {
      dispatchToListeners(msg);
    }
  }

  private void onConnectionTransportFailure() {
    clusterWakeupReceived("transport_failure");
  }

  private void clusterWakeupPublished(String outcome) {
    metrics.clusterWakeupPublished(COORDINATOR_KIND, outcome);
  }

  private void clusterWakeupReceived(String outcome) {
    metrics.clusterWakeupReceived(COORDINATOR_KIND, outcome);
  }

  private JmsConnectionFactoryProvider resolveProvider() {
    Instance<JmsConnectionFactoryProvider> instance = this.providerInstance;
    if (instance == null || instance.isUnsatisfied()) {
      throw new IllegalStateException(
          "No JmsConnectionFactoryProvider available for JmsClusterCoordinator. Provide a"
              + " @Produces JmsConnectionFactoryProvider or use the default JNDI lookup bean.");
    }
    if (instance.isAmbiguous()) {
      log.warn(
          "Multiple JmsConnectionFactoryProvider beans visible; first match wins. Use"
              + " @Alternative + @Priority for disambiguation.");
    }
    return instance.get();
  }

  /**
   * Test accessor: exposes the active connection lifecycle so the TCK harness can poll readiness.
   */
  JmsConnectionLifecycle lifecycle() {
    return connectionLifecycle;
  }

  private ExecutorService newListenerExecutor() {
    ThreadFactory tf =
        new ThreadFactory() {
          private final AtomicLong counter = new AtomicLong();

          @Override
          public Thread newThread(Runnable r) {
            Thread t =
                new Thread(r, "ratchet-coordinator-jms-dispatch-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          }
        };
    return Executors.newFixedThreadPool(Math.max(1, config.listenerExecutorThreads()), tf);
  }
}
