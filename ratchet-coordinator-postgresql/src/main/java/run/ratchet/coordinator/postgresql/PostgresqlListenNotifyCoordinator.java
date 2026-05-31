package run.ratchet.coordinator.postgresql;

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import javax.sql.DataSource;
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
 * PostgreSQL {@code LISTEN}/{@code NOTIFY}-based {@link ClusterCoordinator}.
 *
 * <p>Adding this module to the deployment activates push-based cross-node wakeups in place of the
 * default {@code NoOpClusterCoordinator}. Activation is via {@link Alternative} + {@link Priority}
 * — per CDI 4.0 §4.1.1 a {@code @Priority}-annotated alternative is selected globally across all
 * archives, so consumers do not need to edit application-side {@code beans.xml}.
 *
 * <p>The dedicated {@link PGConnection} acquired in {@link #init()} is single-purpose: it is
 * autocommit, never returned to a pool, and used only by the LISTEN thread. Outbound {@code
 * pg_notify} calls borrow short-lived publish connections from the configured {@link DataSource} so
 * publish statements cannot stall behind {@code getNotifications(receiveTimeoutMs)} on the LISTEN
 * connection.
 *
 * <p>{@link #close()} releases only resources this coordinator allocated: the {@link
 * PostgresqlConnectionLifecycle} and the listener {@link ExecutorService}. The {@link DataSource}
 * itself is provider-owned and is never closed by this class.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 400)
// Coordinator @Priority order (highest wins per CDI 4.0 §5.2.4):
//   PG = PLATFORM_BEFORE + 400, JMS = +300, Hazelcast = +200, Infinispan = +100.
// Operators MUST pull in exactly one coordinator module; distinct priorities only mean a
// transitive double-pull picks PG over the others — it does not legitimize the configuration.
public class PostgresqlListenNotifyCoordinator
    implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(PostgresqlListenNotifyCoordinator.class);

  /** Bounded buffer holding inbound messages that arrive before any listener registers. */
  private static final int PRE_REGISTRATION_BUFFER_CAPACITY = 256;

  private static final String COORDINATOR_KIND = "postgresql";

  @Inject NodeIdentityProvider identityProvider;

  /**
   * Resolved lazily in {@link #init()}. The config record is a plain value type with a {@code
   * defaults()} factory, not a managed bean, so it is injected as an {@link Instance} and the
   * coordinator falls back to {@link PostgresqlCoordinatorConfig#defaults()} when no application
   * producer is present. A direct {@code @Inject PostgresqlCoordinatorConfig} would be an
   * unsatisfied dependency and fail deployment validation out of the box.
   */
  @Inject Instance<PostgresqlCoordinatorConfig> configInstance;

  @Inject @Any Instance<DataSource> dataSourceInstance;
  @Inject MetricsCollector metrics;

  private PostgresqlCoordinatorConfig config;

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private final CopyOnWriteArrayList<Consumer<JobWakeupHint>> listeners =
      new CopyOnWriteArrayList<>();
  private final BlockingQueue<NotifyPayload> preRegistrationBuffer =
      new ArrayBlockingQueue<>(PRE_REGISTRATION_BUFFER_CAPACITY);

  private PostgresqlConnectionLifecycle connectionLifecycle;
  private PostgresqlConnectionLifecycle.ConnectionAcquirer publishConnectionAcquirer;
  private PostgresqlListenThread listenThread;
  private ExecutorService listenerExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** CDI proxy constructor — not for direct use. */
  @SuppressWarnings("unused")
  protected PostgresqlListenNotifyCoordinator() {
    // CDI proxy constructor.
  }

  /**
   * Test/non-CDI constructor. Bypasses {@link DataSource} resolution by accepting a fully-formed
   * {@link PostgresqlConnectionLifecycle}. Callers must still invoke {@link #init()} before any SPI
   * method.
   */
  PostgresqlListenNotifyCoordinator(
      NodeIdentityProvider identityProvider,
      PostgresqlCoordinatorConfig config,
      PostgresqlConnectionLifecycle lifecycle,
      MetricsCollector metrics) {
    this(identityProvider, config, lifecycle, lifecycle::currentRaw, metrics);
  }

  PostgresqlListenNotifyCoordinator(
      NodeIdentityProvider identityProvider,
      PostgresqlCoordinatorConfig config,
      PostgresqlConnectionLifecycle lifecycle,
      PostgresqlConnectionLifecycle.ConnectionAcquirer publishConnectionAcquirer,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.connectionLifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    this.publishConnectionAcquirer =
        Objects.requireNonNull(publishConnectionAcquirer, "publishConnectionAcquirer");
    this.metrics = metrics;
  }

  @PostConstruct
  void init() {
    if (config == null) {
      config =
          configInstance != null && configInstance.isResolvable()
              ? configInstance.get()
              : PostgresqlCoordinatorConfig.defaults();
    }
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    DataSource ds = null;
    if (connectionLifecycle == null || publishConnectionAcquirer == null) {
      ds = resolveDataSource();
    }
    if (connectionLifecycle == null) {
      connectionLifecycle = new PostgresqlConnectionLifecycle(ds, config);
    }
    if (publishConnectionAcquirer == null) {
      DataSource publishDataSource = ds;
      publishConnectionAcquirer = publishDataSource::getConnection;
    }
    listenerExecutor = newListenerExecutor();
    listenThread =
        new PostgresqlListenThread(
            connectionLifecycle,
            codec,
            config,
            this::onInboundNotification,
            this::onParseFailure,
            this::onTransportFailure);
  }

  /** {@inheritDoc} */
  @Override
  public void afterStart() {
    if (closed.get()) {
      return;
    }
    if (listenThread == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    if (listenThread.getState() == Thread.State.NEW) {
      listenThread.start();
    }
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (closed.get()) {
      return;
    }
    try {
      String payload = codec.encode(NotifyPayload.current(source, priority, executionTarget));
      try (Connection raw = publishConnectionAcquirer.acquire();
          PreparedStatement ps = raw.prepareStatement("SELECT pg_notify(?, ?)")) {
        ps.setString(1, config.effectiveChannel());
        ps.setString(2, payload);
        ps.execute();
      }
      clusterWakeupPublished("success");
    } catch (SQLException sqlEx) {
      clusterWakeupPublished("failure");
      log.warnf(
          "PostgreSQL coordinator notifyNewWork transport failure: %s — wakeup dropped",
          sqlEx.getMessage());
      // intentionally swallow — never throw out of notifyNewWork
    } catch (RuntimeException encodeEx) {
      // The codec or JSON-P provider can raise JsonException etc. The SPI contract is
      // "notifyNewWork never throws"; swallow and metric.
      clusterWakeupPublished("failure");
      log.warnf(
          encodeEx,
          "PostgreSQL coordinator notifyNewWork encode/dispatch failure: %s — wakeup dropped",
          encodeEx.getMessage());
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
    PostgresqlListenThread thread = this.listenThread;
    if (thread != null) {
      thread.shutdown();
    }
    PostgresqlConnectionLifecycle lifecycle = this.connectionLifecycle;
    if (lifecycle != null) {
      lifecycle.close();
    }
    if (thread != null) {
      try {
        thread.join(config.shutdownGraceMs());
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
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

  /**
   * Package-private test seam: synthesizes the same dispatch a real PostgreSQL {@code NOTIFY} would
   * trigger, without standing up a live LISTEN connection. Unit tests in this package use this hook
   * to drive {@link #onInboundNotification(NotifyPayload)} without going through Testcontainers.
   *
   * @apiNote Framework-internal test driver. Not part of the public SPI.
   */
  void dispatchInbound(NotifyPayload msg) {
    onInboundNotification(msg);
  }

  /** Dispatch path from the listen thread. Self-suppresses then routes to listeners. */
  private void onInboundNotification(NotifyPayload msg) {
    NodeIdentity local;
    try {
      local = new NodeIdentity(identityProvider.getNodeId());
    } catch (RuntimeException e) {
      log.warnf("PostgreSQL coordinator: NodeIdentityProvider error: %s", e.getMessage());
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
                    "PostgreSQL coordinator listener threw: %s — suppressing per SPI contract",
                    listenerEx.getMessage());
              }
            });
      } catch (RuntimeException submitEx) {
        // Executor refused (shutdown or saturated). Skip; the local poller floor is unaffected.
        log.debugf(
            submitEx,
            "PostgreSQL coordinator could not enqueue listener task: %s",
            submitEx.getMessage());
      }
    }
  }

  // Drop oldest and try once more. Counts as overflow regardless of the second offer's outcome.
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void bufferOrDropOldest(NotifyPayload msg) {
    if (preRegistrationBuffer.offer(msg)) {
      return;
    }
    preRegistrationBuffer.poll();
    preRegistrationBuffer.offer(msg);
    clusterWakeupReceived("pre_registration_overflow");
    log.warn("PostgreSQL coordinator pre-registration buffer overflowed; oldest wakeup dropped");
  }

  private void drainPreRegistrationBuffer() {
    // Drain after the listener is visible so buffered wakeups reach the first subscriber.
    // A notify can still arrive during concurrent listener registration and get buffered
    // for the next registration cycle. The local poll loop is the correctness floor; the
    // SPI wakeup signal is best-effort.
    NotifyPayload msg;
    while ((msg = preRegistrationBuffer.poll()) != null) {
      dispatchToListeners(msg);
    }
  }

  private void onParseFailure() {
    clusterWakeupReceived("parse_failure");
  }

  private void onTransportFailure() {
    clusterWakeupReceived("transport_failure");
  }

  private void clusterWakeupPublished(String outcome) {
    metrics.clusterWakeupPublished(COORDINATOR_KIND, outcome);
  }

  private void clusterWakeupReceived(String outcome) {
    metrics.clusterWakeupReceived(COORDINATOR_KIND, outcome);
  }

  private DataSource resolveDataSource() {
    Instance<DataSource> instance = this.dataSourceInstance;
    if (instance == null || instance.isUnsatisfied()) {
      throw new IllegalStateException(
          "No DataSource available for PostgresqlListenNotifyCoordinator. Provide a @Produces"
              + " DataSource or wire one via container-managed JNDI.");
    }
    if (instance.isAmbiguous()) {
      log.warn(
          "Multiple DataSource beans visible to PostgresqlListenNotifyCoordinator; first match"
              + " wins. Provide a @CoordinatorDataSource qualifier in a future revision for"
              + " disambiguation.");
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
                    r, "ratchet-coordinator-postgresql-dispatch-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          }
        };
    int threads = Math.max(1, config.listenerExecutorThreads());
    return Executors.newFixedThreadPool(threads, tf);
  }
}
