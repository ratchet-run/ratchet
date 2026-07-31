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
package run.ratchet.ri.runtime;

import java.time.Clock;
import java.util.Objects;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.DefaultRetryPolicy;
import run.ratchet.ri.cdi.NoOpClusterCoordinator;
import run.ratchet.ri.cdi.NoOpTracingCollector;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;
import run.ratchet.ri.core.DefaultExecutionTuningProvider;
import run.ratchet.ri.core.DefaultPollingStrategyProvider;
import run.ratchet.ri.core.internal.DefaultNodeTagAffinityProvider;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.DefaultCircuitBreakerConfigProvider;
import run.ratchet.ri.resilience.DefaultResilienceStrategy;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.ri.security.PermitAllJobAuthorizationPolicy;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerManager;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.TracingCollector;

/** Secure, container-neutral defaults for integrations that manage the Ratchet runtime graph. */
public final class RatchetRuntimeDefaults {

  private static final Logger LOG = Logger.getLogger(RatchetRuntimeDefaults.class);

  private RatchetRuntimeDefaults() {}

  public static ClassPolicy classPolicy(RatchetOptions options) {
    Objects.requireNonNull(options, "options must not be null");
    RatchetOptions.SecurityOptions security = options.security();
    PackagePrefixClassPolicy policy =
        new PackagePrefixClassPolicy(
            security.classPolicyAllowedPackages(), security.classPolicyAllowedResultTypePackages());
    if (security.classPolicyAllowedPackages().isEmpty()) {
      if (!security.allowEmptyClassPolicy()) {
        String message =
            "ClassPolicy invocation allowlist is empty - refusing to start. Configure "
                + "ratchet.class-policy.allowed-packages or set "
                + "ratchet.allow-empty-class-policy=true ONLY for demos/tests.";
        LOG.error(message);
        throw new IllegalStateException(message);
      }
      LOG.error(
          "ClassPolicy invocation allowlist is empty because "
              + "ratchet.allow-empty-class-policy=true; ALL job targets will be rejected.");
    }
    return policy;
  }

  public static ExecutorProvider executorProvider() {
    return new StandaloneExecutorProvider();
  }

  public static MetricsCollector metricsCollector() {
    return new NoOpMetricsCollector();
  }

  public static TracingCollector tracingCollector() {
    return new NoOpTracingCollector();
  }

  public static ClusterCoordinator clusterCoordinator() {
    return new NoOpClusterCoordinator();
  }

  public static ErrorSanitizer errorSanitizer(RatchetOptions options) {
    Objects.requireNonNull(options, "options must not be null");
    return new DefaultErrorSanitizer(options.security().redactEmails());
  }

  public static NodeTagAffinityProvider nodeTagAffinityProvider(RatchetOptions options) {
    return new DefaultNodeTagAffinityProvider(
        Objects.requireNonNull(options, "options must not be null"));
  }

  public static CircuitBreakerConfigProvider circuitBreakerConfigProvider(RatchetOptions options) {
    return new DefaultCircuitBreakerConfigProvider(
        Objects.requireNonNull(options, "options must not be null"));
  }

  public static CircuitBreakerManager circuitBreakerRegistry(RatchetOptions options) {
    return circuitBreakerRegistry(options, metricsCollector());
  }

  public static CircuitBreakerManager circuitBreakerRegistry(
      RatchetOptions options, MetricsCollector metricsCollector) {
    return circuitBreakerRegistry(circuitBreakerConfigProvider(options), metricsCollector);
  }

  public static CircuitBreakerManager circuitBreakerRegistry(
      CircuitBreakerConfigProvider circuitBreakerConfigProvider,
      MetricsCollector metricsCollector) {
    return new CircuitBreakerRegistry(
        Objects.requireNonNull(
            circuitBreakerConfigProvider, "circuitBreakerConfigProvider must not be null"),
        Objects.requireNonNull(metricsCollector, "metricsCollector must not be null"));
  }

  public static ResilienceStrategy resilienceStrategy(
      CircuitBreakerManager circuitBreakerRegistry, RatchetOptions options) {
    return resilienceStrategy(circuitBreakerRegistry, circuitBreakerConfigProvider(options));
  }

  public static ResilienceStrategy resilienceStrategy(
      CircuitBreakerManager circuitBreakerRegistry,
      CircuitBreakerConfigProvider circuitBreakerConfigProvider) {
    return new DefaultResilienceStrategy(
        Objects.requireNonNull(circuitBreakerRegistry, "circuitBreakerRegistry must not be null"),
        Objects.requireNonNull(
            circuitBreakerConfigProvider, "circuitBreakerConfigProvider must not be null"));
  }

  public static Clock clock() {
    return Clock.systemUTC();
  }

  public static JobAuthorizationPolicy jobAuthorizationPolicy() {
    return new PermitAllJobAuthorizationPolicy();
  }

  public static JobInvocationResolver jobInvocationResolver() {
    return new DefaultJobInvocationResolver();
  }

  public static PollingStrategyProvider pollingStrategyProvider() {
    return new DefaultPollingStrategyProvider();
  }

  public static ExecutionTuningProvider executionTuningProvider(RatchetOptions options) {
    return new DefaultExecutionTuningProvider(
        Objects.requireNonNull(options, "options must not be null"));
  }

  public static RetryPolicy retryPolicy() {
    return new DefaultRetryPolicy();
  }
}
