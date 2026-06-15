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
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.AbstractPushCoordinator;
import run.ratchet.coordinator.common.CoordinatorSupport;
import run.ratchet.coordinator.common.CoordinatorThreading;
import run.ratchet.coordinator.common.NotifyPayload;
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
public class PostgresqlListenNotifyCoordinator extends AbstractPushCoordinator
    implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(PostgresqlListenNotifyCoordinator.class);

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

  private PostgresqlConnectionLifecycle connectionLifecycle;
  private PostgresqlConnectionLifecycle.ConnectionAcquirer publishConnectionAcquirer;
  private PostgresqlListenThread listenThread;
  private Thread listenThreadHandle;
  private CoordinatorThreading threading;

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
    this.threading = CoordinatorThreading.standalone("ratchet-coordinator-postgresql");
  }

  @PostConstruct
  void init() {
    if (config == null) {
      config =
          CoordinatorSupport.resolveConfigOrDefault(
              configInstance, PostgresqlCoordinatorConfig::defaults);
    }
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    if (threading == null) {
      // CDI/production path: route the LISTEN loop and the dispatch pool through the container's
      // managed thread factory. Standalone is an explicit opt-in via the test constructors.
      threading = CoordinatorThreading.managed("ratchet-coordinator-postgresql");
    }
    configureDispatch(
        COORDINATOR_KIND,
        "PostgreSQL",
        metrics,
        identityProvider,
        config.maxInboundPayloadChars(),
        threading.newDispatchPool(
            "dispatch", config.listenerExecutorThreads(), config.listenerExecutorQueueCapacity()),
        config.shutdownGraceMs());
    DataSource ds = null;
    if (connectionLifecycle == null || publishConnectionAcquirer == null) {
      ds =
          CoordinatorSupport.resolveRequired(
              dataSourceInstance,
              "No DataSource available for PostgresqlListenNotifyCoordinator. Provide a @Produces"
                  + " DataSource or wire one via container-managed JNDI.",
              "Multiple DataSource beans visible to PostgresqlListenNotifyCoordinator; first match"
                  + " wins. Provide a @CoordinatorDataSource qualifier in a future revision for"
                  + " disambiguation.");
    }
    if (connectionLifecycle == null) {
      connectionLifecycle = new PostgresqlConnectionLifecycle(ds, config);
    }
    if (publishConnectionAcquirer == null) {
      DataSource publishDataSource = ds;
      publishConnectionAcquirer = publishDataSource::getConnection;
    }
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
    if (isClosed()) {
      return;
    }
    if (listenThread == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    if (listenThreadHandle == null) {
      listenThreadHandle = threading.newLoopThread("listen", listenThread);
      listenThreadHandle.start();
    }
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (isClosed()) {
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
    if (!markClosed()) {
      return;
    }
    PostgresqlListenThread thread = this.listenThread;
    if (thread != null) {
      thread.shutdown();
    }
    Thread handle = this.listenThreadHandle;
    if (handle != null) {
      handle.interrupt();
    }
    PostgresqlConnectionLifecycle lifecycle = this.connectionLifecycle;
    if (lifecycle != null) {
      lifecycle.close();
    }
    if (handle != null) {
      try {
        handle.join(config.shutdownGraceMs());
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
    shutdownListenerExecutor();
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
    deliverDecodedPayload(msg);
  }

  @Override
  protected void onNodeIdentityProviderError(RuntimeException e) {
    log.warnf("PostgreSQL coordinator: NodeIdentityProvider error: %s", e.getMessage());
  }

  private void onParseFailure() {
    clusterWakeupReceived("parse_failure");
  }

  private void onTransportFailure() {
    clusterWakeupReceived("transport_failure");
  }
}
