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
package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;
import java.time.Clock;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.internal.ExecutionObserver;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.JobTimeoutHandler;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.RuntimeInstallation;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.runtime.RatchetRuntimeDefaults;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.PrincipalSource;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.SignalStore;

/**
 * CDI producer for Ratchet beans that require configuration values mixed with injectable
 * dependencies. Configuration is read from the injected {@link RatchetOptions} bean.
 *
 * <p>Beans with purely injectable constructors are annotated directly with
 * {@code @ApplicationScoped} and {@code @Inject}. This producer handles the remaining beans whose
 * constructors require primitive configuration parameters.
 */
@ApplicationScoped
public class RatchetProducer {

  private static final Logger log = Logger.getLogger(RatchetProducer.class);

  private final ExecutorProvider executorProvider;
  private final MetricsCollector metricsCollector;
  private final TracingCollector tracingCollector;
  private final JobCrudStore jobCrudStore;
  private final JobRetryStore jobRetryStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final PostExecutionHandler postExecutionHandler;
  private final NodeStore nodeStore;
  private final RatchetOptions options;
  private final ExecutionTuningProvider executionTuningProvider;
  private final PollingStrategyProvider pollingStrategyProvider;
  private final CircuitBreakerConfigProvider circuitBreakerConfigProvider;
  private volatile Instance.Handle<PayloadSerializer> dependentPayloadSerializerHandle;
  private volatile RuntimeInstallation payloadSerializerInstallation;
  private volatile Object payloadSerializerOwnerToken;

  protected RatchetProducer() {
    this.executorProvider = null;
    this.metricsCollector = null;
    this.tracingCollector = null;
    this.jobCrudStore = null;
    this.jobRetryStore = null;
    this.jobBatchStatusStore = null;
    this.postExecutionHandler = null;
    this.nodeStore = null;
    this.options = null;
    this.executionTuningProvider = null;
    this.pollingStrategyProvider = null;
    this.circuitBreakerConfigProvider = null;
  }

  @Inject
  public RatchetProducer(
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      TracingCollector tracingCollector,
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler postExecutionHandler,
      NodeStore nodeStore,
      RatchetOptions options,
      ExecutionTuningProvider executionTuningProvider,
      PollingStrategyProvider pollingStrategyProvider,
      CircuitBreakerConfigProvider circuitBreakerConfigProvider) {
    this.executorProvider = executorProvider;
    this.metricsCollector = metricsCollector;
    this.tracingCollector = tracingCollector;
    this.jobCrudStore = jobCrudStore;
    this.jobRetryStore = jobRetryStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.postExecutionHandler = postExecutionHandler;
    this.nodeStore = nodeStore;
    this.options = options;
    this.executionTuningProvider = executionTuningProvider;
    this.pollingStrategyProvider = pollingStrategyProvider;
    this.circuitBreakerConfigProvider = circuitBreakerConfigProvider;
  }

  @Produces
  @ApplicationScoped
  public PoolRegistry poolRegistry() {
    return new PoolRegistry(options, executorProvider, metricsCollector, executionTuningProvider);
  }

  @Produces
  @ApplicationScoped
  public JobTimeoutHandler jobTimeoutHandler(
      Clock clock,
      InternalEventPublisher eventPublisher,
      Instance<SignalStore> signalStore,
      SingletonLeaseService singletonLeaseService,
      ErrorSanitizer errorSanitizer,
      AfterCommitRegistrar afterCommitRegistrar) {
    return new JobTimeoutHandler(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        postExecutionHandler,
        clock,
        eventPublisher,
        signalStore.isResolvable() ? signalStore.get() : null,
        metricsCollector,
        afterCommitRegistrar,
        singletonLeaseService,
        errorSanitizer,
        options);
  }

  @Produces
  @ApplicationScoped
  public DynamicHeartbeatCalculator dynamicHeartbeatCalculator() {
    return new DynamicHeartbeatCalculator(jobCrudStore, options, Clock.systemUTC());
  }

  @Produces
  @ApplicationScoped
  public DefaultNodeIdentityProvider nodeIdentityProvider(
      DynamicHeartbeatCalculator heartbeatCalculator, JobBulkStore jobBulkStore, Clock clock) {
    return new DefaultNodeIdentityProvider(
        nodeStore, jobBulkStore, heartbeatCalculator, executorProvider, options, clock);
  }

  @Produces
  @ApplicationScoped
  public Poller poller(
      JobClaimStore jobClaimStore,
      JobExecutionCoordinator jobExecutionCoordinator,
      NodeIdentityProvider nodeIdProvider,
      PoolRegistry poolRegistry,
      DrainController drainController,
      PollerScheduler pollerScheduler,
      CircuitBreakerRegistry circuitBreakerRegistry,
      NodeTagAffinityProvider tagAffinityProvider,
      JobTimeoutHandler timeoutHandler) {
    return new Poller(
        jobClaimStore,
        jobExecutionCoordinator,
        nodeIdProvider,
        poolRegistry,
        drainController,
        pollerScheduler,
        options,
        metricsCollector,
        circuitBreakerRegistry,
        circuitBreakerConfigProvider,
        pollingStrategyProvider,
        tagAffinityProvider,
        timeoutHandler);
  }

  @Produces
  @ApplicationScoped
  public ExecutionObserver executionObserver(
      InternalEventPublisher eventPublisher, Instance<JobAuditStore> executionStore) {
    // delayedJobReadyCallback is null by default; can be wired later if needed
    return new ExecutionObserver(
        metricsCollector,
        tracingCollector,
        eventPublisher,
        executionStore.isResolvable() ? executionStore.get() : null,
        executorProvider);
  }

  @Produces
  @ApplicationScoped
  public OrphanRecoveryTimer orphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      ResourcePermitService resourcePermitService,
      SingletonLeaseService singletonLeaseService) {
    return new OrphanRecoveryTimer(
        jobBulkStore,
        nodeStore,
        resourcePermitService,
        singletonLeaseService,
        options,
        Clock.systemUTC());
  }

  /**
   * Produces the default {@link ClassPolicy} bean. Users override by providing their own
   * {@code @Alternative @Priority(APPLICATION) ClassPolicy} bean.
   *
   * <p><b>Fail-fast:</b> if no allowlist is configured, this producer throws {@link
   * DeploymentException} at container startup. The default policy with an empty allowlist is a
   * deny-all configuration that would prevent any job from running. Set {@link
   * RatchetOptions.SecurityBuilder#allowEmptyClassPolicy} only for demos, fixtures, or tests that
   * deliberately want a deny-all policy.
   */
  @Produces
  @Default
  @ApplicationScoped
  public ClassPolicy classPolicy() {
    try {
      return RatchetRuntimeDefaults.classPolicy(options);
    } catch (IllegalStateException e) {
      String message =
          e.getMessage()
              + " In CDI, an @Alternative @Priority(APPLICATION) ClassPolicy bean may provide "
              + "the application's package policy instead.";
      throw new DeploymentException(message, e);
    }
  }

  /**
   * Produces the default {@link CallerPrincipalProvider} bean that resolves the caller principal
   * from platform {@link PrincipalSource} beans. Users can override by providing their own
   * {@code @Alternative @Priority(APPLICATION) CallerPrincipalProvider} bean.
   */
  @Produces
  @Default
  @ApplicationScoped
  public CallerPrincipalProvider callerPrincipalProvider(Instance<PrincipalSource> sources) {
    return new CallerPrincipalProvider(sources);
  }

  /**
   * Produces the default {@link ErrorSanitizer} bean that strips PII and credentials from exception
   * messages before persistence. Users can override by providing their own
   * {@code @Alternative @Priority(APPLICATION) ErrorSanitizer} bean.
   */
  @Produces
  @Default
  @ApplicationScoped
  public ErrorSanitizer errorSanitizer() {
    return RatchetRuntimeDefaults.errorSanitizer(options);
  }

  /**
   * Produces the default {@link ResilienceStrategy} bean backed by the built-in circuit breaker.
   * Users can override by providing their own {@code @Alternative @Priority(APPLICATION)
   * ResilienceStrategy} bean (e.g., backed by Resilience4j or MicroProfile Fault Tolerance).
   */
  @Produces
  @Default
  @ApplicationScoped
  public ResilienceStrategy resilienceStrategy(CircuitBreakerRegistry circuitBreakerRegistry) {
    return RatchetRuntimeDefaults.resilienceStrategy(
        circuitBreakerRegistry, circuitBreakerConfigProvider);
  }

  /**
   * Produces the default {@link Clock} bean used by scheduling code that computes job {@code
   * scheduledTime}. Tests override with a deterministic clock by providing an
   * {@code @Alternative @Priority(APPLICATION) Clock} bean (e.g., {@code SteppingTestClock} from
   * {@code ratchet-tck-api}).
   */
  @Produces
  @Default
  @ApplicationScoped
  public Clock systemClock() {
    return RatchetRuntimeDefaults.clock();
  }

  /**
   * Produces the default {@link NodeTagAffinityProvider} bean. Users can override by providing
   * their own {@code @Alternative @Priority(APPLICATION) NodeTagAffinityProvider} bean for
   * runtime-dynamic tag affinity (e.g., based on hardware availability).
   */
  @Produces
  @Default
  @ApplicationScoped
  public NodeTagAffinityProvider nodeTagAffinityProvider() {
    return RatchetRuntimeDefaults.nodeTagAffinityProvider(options);
  }

  public RuntimeInstallation payloadSerializerInstallation(
      Instance<PayloadSerializer> payloadSerializers) {
    RuntimeInstallation current = payloadSerializerInstallation;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (payloadSerializerInstallation == null) {
        payloadSerializerInstallation =
            new RuntimeInstallation() {
              @Override
              public void install(Object ownerToken) {
                if (!payloadSerializers.isResolvable()) {
                  log.warn(
                      "No PayloadSerializer bean resolvable at startup; JPA converters will use"
                          + " fallback JSON-B.");
                  PayloadSerializerHolder.install(ownerToken, null);
                  payloadSerializerOwnerToken = ownerToken;
                  return;
                }

                destroyDependentPayloadSerializer();
                Instance.Handle<PayloadSerializer> handle = payloadSerializers.getHandle();
                boolean dependent = handle.getBean().getScope().equals(Dependent.class);
                try {
                  PayloadSerializerHolder.install(ownerToken, handle.get());
                } catch (RuntimeException | Error failure) {
                  if (dependent) {
                    destroyPayloadSerializerHandle(handle);
                  }
                  throw failure;
                }
                if (dependent) {
                  dependentPayloadSerializerHandle = handle;
                }
                payloadSerializerOwnerToken = ownerToken;
              }

              @Override
              public void uninstall(Object ownerToken) {
                PayloadSerializerHolder.uninstall(ownerToken);
              }
            };
      }
      return payloadSerializerInstallation;
    }
  }

  @PreDestroy
  void unregisterPayloadSerializer() {
    Object ownerToken = payloadSerializerOwnerToken;
    if (ownerToken != null) {
      PayloadSerializerHolder.uninstall(ownerToken);
    }
    destroyDependentPayloadSerializer();
  }

  private void destroyDependentPayloadSerializer() {
    Instance.Handle<PayloadSerializer> handle = dependentPayloadSerializerHandle;
    dependentPayloadSerializerHandle = null;
    if (handle != null) {
      destroyPayloadSerializerHandle(handle);
    }
  }

  private void destroyPayloadSerializerHandle(Instance.Handle<PayloadSerializer> handle) {
    try {
      handle.destroy();
    } catch (RuntimeException e) {
      log.warnf(e, "PayloadSerializer destruction failed during Ratchet shutdown");
    }
  }
}
