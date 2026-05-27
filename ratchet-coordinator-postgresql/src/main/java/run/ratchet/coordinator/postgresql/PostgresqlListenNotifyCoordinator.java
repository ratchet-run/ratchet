package run.ratchet.coordinator.postgresql;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import jakarta.json.JsonException;
import jakarta.json.spi.JsonProvider;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.sql.DataSource;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.postgresql.PostgresqlNotifyPayloadCodec.NotifyPayload;
import run.ratchet.spi.ClusterCoordinator;
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
 * <p>The dedicated {@link PGConnection} acquired in {@link #init()} is autocommit and never
 * returned to a pool. Outbound {@code pg_notify} and the LISTEN thread share that single
 * connection; the PostgreSQL JDBC driver serializes statement execution per-connection, so
 * concurrency is safe but outbound throughput is bounded by {@link
 * PostgresqlCoordinatorConfig#receiveTimeoutMs()} (see config Javadoc).
 *
 * <p>{@link #close()} releases only resources this coordinator allocated: the {@link
 * PostgresqlConnectionLifecycle} and the listener {@link ExecutorService}. The {@link DataSource}
 * itself is provider-owned and is never closed by this class.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 100)
// Coordinator @Priority order (lowest wins per CDI 4.0 §5.2.4):
//   PG = PLATFORM_BEFORE + 100, JMS = +200, Hazelcast = +300, Infinispan = +400.
// Operators MUST pull in exactly one coordinator module; distinct priorities only mean a
// transitive double-pull picks PG over the others — it does not legitimize the configuration.
public class PostgresqlListenNotifyCoordinator
    implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(PostgresqlListenNotifyCoordinator.class);

  /** Bounded buffer holding inbound messages that arrive before any listener registers. */
  private static final int PRE_REGISTRATION_BUFFER_CAPACITY = 256;

  static final String COORDINATOR_KIND = "postgresql";

  @Inject NodeIdentityProvider identityProvider;
  @Inject PostgresqlCoordinatorConfig config;
  @Inject @Any Instance<DataSource> dataSourceInstance;
  @Inject Instance<MetricsCollector> metricsInstance;

  private final PostgresqlNotifyPayloadCodec codec = new PostgresqlNotifyPayloadCodec();
  private final CopyOnWriteArrayList<BiConsumer<JobPriority, NodeIdentity>> listeners =
      new CopyOnWriteArrayList<>();
  private final BlockingQueue<NotifyPayload> preRegistrationBuffer =
      new ArrayBlockingQueue<>(PRE_REGISTRATION_BUFFER_CAPACITY);

  private PostgresqlConnectionLifecycle connectionLifecycle;
  private PostgresqlListenThread listenThread;
  private ExecutorService listenerExecutor;
  private MetricsCollector metrics;
  private volatile boolean closed;

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
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.connectionLifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    this.metrics = metrics;
  }

  @PostConstruct
  void init() {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    if (connectionLifecycle == null) {
      DataSource ds = resolveDataSource();
      connectionLifecycle = new PostgresqlConnectionLifecycle(ds, config);
    }
    if (metrics == null && metricsInstance != null) {
      metrics =
          metricsInstance.isUnsatisfied() || metricsInstance.isAmbiguous()
              ? null
              : metricsInstance.get();
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
    if (closed) {
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
  public void notifyNewWork(JobPriority priority, NodeIdentity source) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (closed) {
      return;
    }
    try {
      java.sql.Connection raw = connectionLifecycle.currentRaw();
      String payload = codec.encode(NotifyPayload.current(source, priority));
      try (PreparedStatement ps = raw.prepareStatement("SELECT pg_notify(?, ?)")) {
        ps.setString(1, config.effectiveChannel());
        ps.setString(2, payload);
        ps.execute();
      }
      clusterWakeupPublished("success");
    } catch (SQLException sqlEx) {
      clusterWakeupPublished("failure");
      connectionLifecycle.markFailed(sqlEx);
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
  public void registerWakeupListener(BiConsumer<JobPriority, NodeIdentity> listener) {
    Objects.requireNonNull(listener, "listener");
    if (closed) {
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
    if (closed) {
      return;
    }
    closed = true;
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

  /** Dispatch path from the listen thread. Self-suppresses then routes to listeners. */
  void onInboundNotification(NotifyPayload msg) {
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
    // Drain after the listener has been added to {@code listeners}, so dispatchToListeners
    // sees the new listener. A narrow concurrent-registration window remains: if a notify
    // arrives between {@code listeners.isEmpty()} returning false in onInboundNotification
    // and a second listener completing registration, that notify may land in the buffer and
    // sit there until the next registerWakeupListener call. The SPI is a best-effort wakeup
    // hint and the local poll loop is the correctness floor, so the delay is benign.
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
    if (metrics != null) {
      try {
        metrics.clusterWakeupPublished(COORDINATOR_KIND, outcome);
      } catch (RuntimeException ignored) {
        // metrics must never destabilize the coordinator
      }
    }
  }

  private void clusterWakeupReceived(String outcome) {
    if (metrics != null) {
      try {
        metrics.clusterWakeupReceived(COORDINATOR_KIND, outcome);
      } catch (RuntimeException ignored) {
        // metrics must never destabilize the coordinator
      }
    }
  }

  /**
   * Fails fast at startup if no JSON-P provider (e.g. parsson) is on the classpath. Without this
   * probe the first {@link #notifyNewWork} call would throw {@link JsonException} from inside the
   * codec; surfacing it here yields a deterministic startup error pointing operators at the missing
   * dependency.
   */
  private static void requireJsonProvider() {
    try {
      JsonProvider.provider();
    } catch (JsonException ex) {
      throw new IllegalStateException(
          "No JSON-P provider (jakarta.json.spi.JsonProvider) found on the classpath. Add"
              + " org.eclipse.parsson:parsson (or another JSON-P 2.x implementation) at runtime"
              + " scope, or deploy into a Jakarta EE container that supplies one.",
          ex);
    }
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
