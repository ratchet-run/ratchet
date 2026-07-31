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

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.ri.runtime.RatchetRuntime;
import run.ratchet.ri.runtime.RatchetRuntimeDefaults;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
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
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.JobStore;

@AutoConfiguration(
    afterName = {
      "run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration",
      "run.ratchet.spring.boot.autoconfigure.mongodb.RatchetMongoAutoConfiguration"
    })
@ConditionalOnProperty(
    name = RatchetProperties.ENABLED_PROPERTY,
    havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties(RatchetProperties.class)
@Import(RatchetBeanDefinitionRegistrar.class)
public class RatchetAutoConfiguration {

  @Bean
  @ConditionalOnBean(JobStore.class)
  RatchetLifecycle ratchetLifecycle(
      ObjectProvider<RatchetRuntime> runtimeProvider, RatchetProperties properties) {
    return new RatchetLifecycle(
        runtimeProvider,
        properties.getLifecycle().getDrainTimeout(),
        properties.getLifecycle().isDeferAutoStart());
  }

  @Bean
  SpringRatchetConfigSource ratchetConfigSource(Environment environment) {
    return new SpringRatchetConfigSource(environment);
  }

  @Bean
  @ConditionalOnMissingBean(RatchetOptions.class)
  RatchetOptions ratchetOptions(SpringRatchetConfigSource configSource) {
    return RatchetOptionsFactory.builderFromEnvironment(configSource).build();
  }

  @Bean
  @ConditionalOnMissingBean(ClassPolicy.class)
  ClassPolicy classPolicy(RatchetOptions options) {
    return RatchetRuntimeDefaults.classPolicy(options);
  }

  @Bean
  @ConditionalOnMissingBean(ExecutorProvider.class)
  ExecutorProvider executorProvider() {
    return RatchetRuntimeDefaults.executorProvider();
  }

  @Bean
  @ConditionalOnMissingBean
  Supplier<ScheduledExecutorService> ratchetScheduledExecutorSupplier(
      ExecutorProvider executorProvider) {
    // Mirrors DefaultRatchetLifecycle's CDI wiring without the JNDI-lookup leg inside
    // DefaultExecutorProvider, which does not apply outside CDI.
    return executorProvider::getScheduledExecutor;
  }

  @Bean
  @ConditionalOnMissingBean(MetricsCollector.class)
  MetricsCollector metricsCollector() {
    return RatchetRuntimeDefaults.metricsCollector();
  }

  @Bean
  @ConditionalOnMissingBean(TracingCollector.class)
  TracingCollector tracingCollector() {
    return RatchetRuntimeDefaults.tracingCollector();
  }

  @Bean
  @ConditionalOnMissingBean(ClusterCoordinator.class)
  ClusterCoordinator clusterCoordinator() {
    return RatchetRuntimeDefaults.clusterCoordinator();
  }

  @Bean
  @ConditionalOnMissingBean(ErrorSanitizer.class)
  ErrorSanitizer errorSanitizer(RatchetOptions options) {
    return RatchetRuntimeDefaults.errorSanitizer(options);
  }

  @Bean
  @ConditionalOnMissingBean(NodeTagAffinityProvider.class)
  NodeTagAffinityProvider nodeTagAffinityProvider(RatchetOptions options) {
    return RatchetRuntimeDefaults.nodeTagAffinityProvider(options);
  }

  @Bean
  @ConditionalOnMissingBean(CircuitBreakerConfigProvider.class)
  CircuitBreakerConfigProvider circuitBreakerConfigProvider(RatchetOptions options) {
    return RatchetRuntimeDefaults.circuitBreakerConfigProvider(options);
  }

  @Bean
  @ConditionalOnMissingBean(CircuitBreakerManager.class)
  CircuitBreakerManager circuitBreakerRegistry(
      CircuitBreakerConfigProvider configProvider, MetricsCollector metricsCollector) {
    return RatchetRuntimeDefaults.circuitBreakerRegistry(configProvider, metricsCollector);
  }

  @Bean
  @ConditionalOnMissingBean(ResilienceStrategy.class)
  ResilienceStrategy resilienceStrategy(
      CircuitBreakerManager circuitBreakerRegistry, CircuitBreakerConfigProvider configProvider) {
    return RatchetRuntimeDefaults.resilienceStrategy(circuitBreakerRegistry, configProvider);
  }

  @Bean
  @ConditionalOnMissingBean(Clock.class)
  Clock clock() {
    return RatchetRuntimeDefaults.clock();
  }

  @Bean
  @ConditionalOnMissingBean(JobAuthorizationPolicy.class)
  JobAuthorizationPolicy jobAuthorizationPolicy() {
    return RatchetRuntimeDefaults.jobAuthorizationPolicy();
  }

  @Bean
  @ConditionalOnMissingBean(JobInvocationResolver.class)
  JobInvocationResolver jobInvocationResolver() {
    return RatchetRuntimeDefaults.jobInvocationResolver();
  }

  @Bean
  @ConditionalOnMissingBean(PollingStrategyProvider.class)
  PollingStrategyProvider pollingStrategyProvider() {
    return RatchetRuntimeDefaults.pollingStrategyProvider();
  }

  @Bean
  @ConditionalOnMissingBean(ExecutionTuningProvider.class)
  ExecutionTuningProvider executionTuningProvider(RatchetOptions options) {
    return RatchetRuntimeDefaults.executionTuningProvider(options);
  }

  @Bean
  @ConditionalOnMissingBean(RetryPolicy.class)
  RetryPolicy retryPolicy() {
    return RatchetRuntimeDefaults.retryPolicy();
  }

  @Bean
  @ConditionalOnMissingBean(PayloadSerializer.class)
  PayloadSerializer payloadSerializer(ObjectProvider<Jsonb> jsonbProvider) {
    Jsonb jsonb = jsonbProvider.getIfAvailable();
    if (jsonb != null) {
      return new SpringJsonbPayloadSerializer(jsonb, false);
    }
    return new SpringJsonbPayloadSerializer(JsonbBuilder.create(), true);
  }

  @Bean
  @Primary
  SpringEventBridge ratchetSpringEventBridge(ApplicationEventPublisher publisher) {
    return new SpringEventBridge(publisher);
  }

  @Bean
  @ConditionalOnMissingBean(AfterCommitRegistrar.class)
  AfterCommitRegistrar afterCommitRegistrar() {
    return new SpringAfterCommitRegistrar();
  }

  @Bean
  @ConditionalOnMissingBean(BeanResolver.class)
  BeanResolver beanResolver(ConfigurableListableBeanFactory beanFactory) {
    return new SpringBeanResolver(beanFactory);
  }

  @Bean
  @ConditionalOnMissingBean(RecurringMethodDiscovery.class)
  RecurringMethodDiscovery recurringMethodDiscovery(ConfigurableListableBeanFactory beanFactory) {
    return new SpringRecurringMethodDiscovery(beanFactory);
  }
}
