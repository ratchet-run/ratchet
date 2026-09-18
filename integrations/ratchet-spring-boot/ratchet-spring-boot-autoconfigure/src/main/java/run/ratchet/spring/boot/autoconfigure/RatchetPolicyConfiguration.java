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
package run.ratchet.spring.boot.autoconfigure;

import java.util.Optional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

/** Internal policy wiring imported by the engine auto-configuration. */
@Configuration(proxyBeanMethods = false)
class RatchetPolicyConfiguration {
  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(RetryPolicy.class)
  DefaultRetryPolicy defaultRetryPolicy() {
    return new DefaultRetryPolicy();
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(CircuitBreakerConfigProvider.class)
  DefaultCircuitBreakerConfigProvider defaultCircuitBreakerConfigProvider(RatchetOptions options) {
    return new DefaultCircuitBreakerConfigProvider(options);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  CircuitBreakerRegistry circuitBreakerRegistry(
      CircuitBreakerConfigProvider configProvider, MetricsCollector metricsCollector) {
    return new CircuitBreakerRegistry(configProvider, metricsCollector);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(JobInvocationResolver.class)
  DefaultJobInvocationResolver defaultJobInvocationResolver() {
    return new DefaultJobInvocationResolver();
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(JobAuthorizationPolicy.class)
  PermitAllJobAuthorizationPolicy permitAllJobAuthorizationPolicy() {
    return new PermitAllJobAuthorizationPolicy();
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  JobPayloadInputValidator jobPayloadInputValidator(RatchetOptions options) {
    return new JobPayloadInputValidator(options);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  JobSecurityValidator jobSecurityValidator(ClassPolicy classPolicy) {
    return new JobSecurityValidator(classPolicy);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(PollingStrategyProvider.class)
  DefaultPollingStrategyProvider defaultPollingStrategyProvider() {
    return new DefaultPollingStrategyProvider();
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(ExecutionTuningProvider.class)
  DefaultExecutionTuningProvider defaultExecutionTuningProvider(RatchetOptions options) {
    return new DefaultExecutionTuningProvider(options);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  JobTypeRateLimiter jobTypeRateLimiter(RatchetOptions options) {
    return new JobTypeRateLimiter(options);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  DoNotRetryPolicy doNotRetryPolicy() {
    return new DoNotRetryPolicy();
  }

  @Bean
  @ConditionalOnMissingBean
  CallerPrincipalProvider callerPrincipalProvider(ObjectProvider<PrincipalSource> sources) {
    return CallerPrincipalProvider.fromSupplier(
        () -> {
          PrincipalSource source = sources.getIfUnique();
          return source == null ? Optional.empty() : source.currentPrincipal();
        });
  }

  @Bean
  @ConditionalOnMissingBean(ErrorSanitizer.class)
  DefaultErrorSanitizer errorSanitizer(RatchetOptions options) {
    return new DefaultErrorSanitizer(options.security().redactEmails());
  }

  @Bean
  @ConditionalOnMissingBean(ResilienceStrategy.class)
  DefaultResilienceStrategy resilienceStrategy(
      CircuitBreakerRegistry registry, CircuitBreakerConfigProvider config) {
    return new DefaultResilienceStrategy(registry, config);
  }

  @Bean
  @ConditionalOnMissingBean(NodeTagAffinityProvider.class)
  DefaultNodeTagAffinityProvider nodeTagAffinityProvider(RatchetOptions options) {
    return new DefaultNodeTagAffinityProvider(options);
  }
}
