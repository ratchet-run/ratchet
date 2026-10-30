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
package run.ratchet.micronaut;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.NonUniqueBeanException;
import jakarta.inject.Singleton;
import java.util.Optional;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.DefaultRetryPolicy;
import run.ratchet.ri.core.DefaultExecutionTuningProvider;
import run.ratchet.ri.core.DefaultPollingStrategyProvider;
import run.ratchet.ri.core.JobTypeRateLimiter;
import run.ratchet.ri.core.internal.DefaultNodeTagAffinityProvider;
import run.ratchet.ri.core.internal.DoNotRetryPolicy;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.DefaultCircuitBreakerConfigProvider;
import run.ratchet.ri.resilience.DefaultResilienceStrategy;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.ri.security.PermitAllJobAuthorizationPolicy;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.PrincipalSource;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.spi.JobStore;

/** Internal policy wiring imported by the engine auto-configuration. */
@Factory
@Requires(beans = JobStore.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
class RatchetPolicyConfiguration {
  @Singleton
  @Requires(missingBeans = RetryPolicy.class)
  RetryPolicy defaultRetryPolicy() {
    return new DefaultRetryPolicy();
  }

  @Singleton
  @Requires(missingBeans = CircuitBreakerConfigProvider.class)
  CircuitBreakerConfigProvider defaultCircuitBreakerConfigProvider(RatchetOptions options) {
    return new DefaultCircuitBreakerConfigProvider(options);
  }

  @Singleton
  @Requires(missingBeans = CircuitBreakerRegistry.class)
  CircuitBreakerRegistry circuitBreakerRegistry(
      CircuitBreakerConfigProvider configProvider, MetricsCollector metricsCollector) {
    return new CircuitBreakerRegistry(configProvider, metricsCollector);
  }

  @Singleton
  @Requires(missingBeans = JobInvocationResolver.class)
  JobInvocationResolver defaultJobInvocationResolver() {
    return new DefaultJobInvocationResolver();
  }

  @Singleton
  @Requires(missingBeans = JobAuthorizationPolicy.class)
  JobAuthorizationPolicy permitAllJobAuthorizationPolicy() {
    return new PermitAllJobAuthorizationPolicy();
  }

  @Singleton
  @Requires(missingBeans = JobPayloadInputValidator.class)
  JobPayloadInputValidator jobPayloadInputValidator(RatchetOptions options) {
    return new JobPayloadInputValidator(options);
  }

  @Singleton
  @Requires(missingBeans = JobSecurityValidator.class)
  JobSecurityValidator jobSecurityValidator(ClassPolicy classPolicy) {
    return new JobSecurityValidator(classPolicy);
  }

  @Singleton
  @Requires(missingBeans = PollingStrategyProvider.class)
  PollingStrategyProvider defaultPollingStrategyProvider() {
    return new DefaultPollingStrategyProvider();
  }

  @Singleton
  @Requires(missingBeans = ExecutionTuningProvider.class)
  ExecutionTuningProvider defaultExecutionTuningProvider(RatchetOptions options) {
    return new DefaultExecutionTuningProvider(options);
  }

  @Singleton
  @Requires(missingBeans = JobTypeRateLimiter.class)
  JobTypeRateLimiter jobTypeRateLimiter(RatchetOptions options) {
    return new JobTypeRateLimiter(options);
  }

  @Singleton
  @Requires(missingBeans = DoNotRetryPolicy.class)
  DoNotRetryPolicy doNotRetryPolicy() {
    return new DoNotRetryPolicy();
  }

  @Singleton
  @Requires(missingBeans = CallerPrincipalProvider.class)
  CallerPrincipalProvider callerPrincipalProvider(BeanProvider<PrincipalSource> sources) {
    return CallerPrincipalProvider.fromSupplier(
        () -> {
          if (!sources.isPresent()) return Optional.empty();
          try {
            return sources.get().currentPrincipal();
          } catch (NonUniqueBeanException ambiguous) {
            return Optional.empty();
          }
        });
  }

  @Singleton
  @Requires(missingBeans = ErrorSanitizer.class)
  ErrorSanitizer errorSanitizer(RatchetOptions options) {
    return new DefaultErrorSanitizer(options.security().redactEmails());
  }

  @Singleton
  @Requires(missingBeans = ResilienceStrategy.class)
  ResilienceStrategy resilienceStrategy(
      CircuitBreakerRegistry registry, CircuitBreakerConfigProvider config) {
    return new DefaultResilienceStrategy(registry, config);
  }

  @Singleton
  @Requires(missingBeans = NodeTagAffinityProvider.class)
  NodeTagAffinityProvider nodeTagAffinityProvider(RatchetOptions options) {
    return new DefaultNodeTagAffinityProvider(options);
  }
}
