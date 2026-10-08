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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import run.ratchet.api.JobQueryService;
import run.ratchet.micrometer.MicrometerMetricTagPolicy;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.micrometer.MicrometerQueueHealthMetrics;
import run.ratchet.micrometer.MicrometerTracingCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;

/** Connects Ratchet metrics and tracing to Spring Boot's observability beans. */
@AutoConfiguration(
    before = RatchetAutoConfiguration.class,
    afterName = {
      "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
      "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
      "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.tracing.BraveAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryTracingAutoConfiguration",
      "org.springframework.boot.micrometer.tracing.autoconfigure.MicrometerTracingAutoConfiguration",
      "org.springframework.boot.micrometer.tracing.brave.autoconfigure.BraveAutoConfiguration",
      "org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.OpenTelemetryTracingAutoConfiguration"
    })
@ConditionalOnClass(MicrometerMetricsCollector.class)
@ConditionalOnProperty(
    prefix = "ratchet",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class RatchetMicrometerAutoConfiguration {
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(MeterRegistry.class)
  @ConditionalOnBean(MeterRegistry.class)
  static class MetricsConfiguration {
    @Bean
    @ConditionalOnMissingBean(MetricsCollector.class)
    MetricsCollector ratchetMicrometerMetricsCollector(
        MeterRegistry registry, ObjectProvider<MicrometerMetricTagPolicy> tagPolicies) {
      MicrometerMetricTagPolicy tagPolicy = tagPolicies.getIfAvailable();
      return tagPolicy == null
          ? new MicrometerMetricsCollector(registry)
          : new MicrometerMetricsCollector(registry, tagPolicy);
    }

    @Bean
    @ConditionalOnMissingBean
    MicrometerQueueHealthMetrics ratchetQueueHealthMetrics(
        MeterRegistry registry, ObjectProvider<JobQueryService> jobQueryService) {
      // The engine registers JobQueryService after this configuration, and not at all without a
      // JobStore; the hook registers no gauges when it is absent.
      return new MicrometerQueueHealthMetrics(registry, jobQueryService.getIfAvailable());
    }
  }

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "io.micrometer.tracing.Tracer")
  static class TracingConfiguration {
    @Bean
    @ConditionalOnBean(Tracer.class)
    @ConditionalOnMissingBean(TracingCollector.class)
    TracingCollector ratchetMicrometerTracingCollector(
        Tracer tracer, ObjectProvider<Propagator> propagator) {
      return new MicrometerTracingCollector(tracer, propagator.getIfAvailable());
    }
  }
}
