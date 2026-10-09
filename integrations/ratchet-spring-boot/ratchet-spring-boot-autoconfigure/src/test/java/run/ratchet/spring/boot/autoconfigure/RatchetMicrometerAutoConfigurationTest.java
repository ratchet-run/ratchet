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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.api.JobQueryService;
import run.ratchet.micrometer.MicrometerMetricTagPolicy;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.micrometer.MicrometerQueueHealthMetrics;
import run.ratchet.micrometer.MicrometerTracingCollector;
import run.ratchet.ri.cdi.NoOpTracingCollector;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.JobStore;

class RatchetMicrometerAutoConfigurationTest {
  @Test
  void meterRegistryEnablesCollectorAndQueueHealthLifecycleHook() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MetricsCollector.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(MicrometerMetricsCollector.class);
              assertThat(context).doesNotHaveBean("ratchetMetricsCollector");
              assertThat(context).hasSingleBean(JobQueryService.class);
              assertThat(context).hasSingleBean(MicrometerQueueHealthMetrics.class);

              MeterRegistry registry = context.getBean(MeterRegistry.class);
              context.getBean(MetricsCollector.class).localWakeup("job_submit");
              assertThat(
                      registry
                          .get("ratchet.wakeup.local")
                          .tag("source", "job_submit")
                          .counter()
                          .count())
                  .isEqualTo(1);
              assertThat(registry.find("ratchet.queue.ready").gauge()).isNull();

              var lifecycleHooks = new SpringLifecycleHooks(context.getBeanFactory());
              var hooks = lifecycleHooks.acquire();
              try {
                assertThat(hooks).contains(context.getBean(MicrometerQueueHealthMetrics.class));
                hooks.forEach(SchedulerLifecycleHook::afterStart);
                assertThat(registry.find("ratchet.queue.ready").gauge()).isNotNull();
              } finally {
                hooks.forEach(SchedulerLifecycleHook::beforeStop);
                hooks.forEach(lifecycleHooks::release);
              }
              assertThat(registry.find("ratchet.queue.ready").gauge()).isNull();
              assertThat(registry.find("ratchet.wakeup.local").counter()).isNotNull();
            });
  }

  @Test
  void missingMeterRegistryKeepsNoOpCollector() {
    runner()
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MetricsCollector.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(NoOpMetricsCollector.class);
              assertThat(context).doesNotHaveBean(MicrometerQueueHealthMetrics.class);
            });
  }

  @Test
  void userMetricsCollectorTakesPrecedenceWithoutDisablingQueueHealth() {
    MetricsCollector custom = mock(MetricsCollector.class);
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean("ratchetMetricsCollector", MetricsCollector.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MetricsCollector.class);
              assertThat(context.getBean(MetricsCollector.class)).isSameAs(custom);
              assertThat(context).doesNotHaveBean("ratchetMicrometerMetricsCollector");
              assertThat(context).hasSingleBean(MicrometerQueueHealthMetrics.class);
            });
  }

  @Test
  void userQueueHealthMetricsTakesPrecedence() {
    MicrometerQueueHealthMetrics custom = mock(MicrometerQueueHealthMetrics.class);
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean(MicrometerQueueHealthMetrics.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MicrometerQueueHealthMetrics.class);
              assertThat(context.getBean(MicrometerQueueHealthMetrics.class)).isSameAs(custom);
              assertThat(context).doesNotHaveBean("ratchetQueueHealthMetrics");
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(MicrometerMetricsCollector.class);
            });
  }

  @Test
  void userTagPolicyExtendsDefaultTags() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean(
            MicrometerMetricTagPolicy.class,
            () -> MicrometerMetricTagPolicy.builder().allowValue("source", "custom").build())
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              MetricsCollector collector = context.getBean(MetricsCollector.class);
              collector.localWakeup("custom");
              collector.localWakeup("job_submit");
              collector.localWakeup("unlisted");

              MeterRegistry registry = context.getBean(MeterRegistry.class);
              for (String source : new String[] {"custom", "job_submit", "OTHER"}) {
                assertThat(
                        registry
                            .get("ratchet.wakeup.local")
                            .tag("source", source)
                            .counter()
                            .count())
                    .isEqualTo(1);
              }
            });
  }

  @Test
  void missingMicrometerCollectorKeepsNoOpCollector() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withClassLoader(new FilteredClassLoader(MicrometerMetricsCollector.class))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MetricsCollector.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(NoOpMetricsCollector.class);
              assertThat(context).doesNotHaveBean(MicrometerQueueHealthMetrics.class);
            });
  }

  @Test
  void missingMicrometerCoreKeepsNoOpCollector() {
    runner()
        .withClassLoader(new FilteredClassLoader("io.micrometer"))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MetricsCollector.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(NoOpMetricsCollector.class);
              assertThat(context).doesNotHaveBean("ratchetQueueHealthMetrics");
            });
  }

  @Test
  void disabledRatchetDoesNotRegisterObservabilityBeans() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean(Tracer.class, () -> Tracer.NOOP)
        .withPropertyValues("ratchet.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean(MetricsCollector.class);
              assertThat(context).doesNotHaveBean(TracingCollector.class);
              assertThat(context).doesNotHaveBean(MicrometerQueueHealthMetrics.class);
            });
  }

  @Test
  void tracerEnablesTracingCollector() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean(Tracer.class, () -> Tracer.NOOP)
        .withBean(Propagator.class, () -> mock(Propagator.class))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(TracingCollector.class);
              assertThat(context.getBean(TracingCollector.class))
                  .isInstanceOf(MicrometerTracingCollector.class);
            });
  }

  @Test
  void tracerWithoutMeterRegistryEnablesTracingOnly() {
    runner()
        .withBean(Tracer.class, () -> Tracer.NOOP)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(TracingCollector.class);
              assertThat(context.getBean(TracingCollector.class))
                  .isInstanceOf(MicrometerTracingCollector.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(NoOpMetricsCollector.class);
              assertThat(context).doesNotHaveBean(MicrometerQueueHealthMetrics.class);
            });
  }

  @Test
  void missingTracerKeepsNoOpTracingCollector() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(TracingCollector.class);
              assertThat(context.getBean(TracingCollector.class))
                  .isInstanceOf(NoOpTracingCollector.class);
            });
  }

  @Test
  void userTracingCollectorTakesPrecedence() {
    TracingCollector custom = mock(TracingCollector.class);
    runner()
        .withBean(Tracer.class, () -> Tracer.NOOP)
        .withBean(TracingCollector.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(TracingCollector.class);
              assertThat(context.getBean(TracingCollector.class)).isSameAs(custom);
              assertThat(context).doesNotHaveBean("ratchetMicrometerTracingCollector");
            });
  }

  @Test
  void missingMicrometerTracingKeepsMetricsAndNoOpTracing() {
    runner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withClassLoader(new FilteredClassLoader("io.micrometer.tracing"))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(TracingCollector.class);
              assertThat(context.getBean(TracingCollector.class))
                  .isInstanceOf(NoOpTracingCollector.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(MicrometerMetricsCollector.class);
              assertThat(context).hasSingleBean(MicrometerQueueHealthMetrics.class);
            });
  }

  @Test
  void meterRegistryWithoutJobStoreStartsWithoutQueueHealthGauges() {
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class,
                RatchetEngineAutoConfiguration.class,
                RatchetMicrometerAutoConfiguration.class))
        .withPropertyValues("ratchet.allowed-packages=example.jobs")
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean(JobQueryService.class);
              assertThat(context.getBean(MetricsCollector.class))
                  .isInstanceOf(MicrometerMetricsCollector.class);

              context.getBean(MicrometerQueueHealthMetrics.class).afterStart();
              assertThat(context.getBean(MeterRegistry.class).find("ratchet.queue.jobs").gauge())
                  .isNull();
            });
  }

  private ApplicationContextRunner runner() {
    return new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class,
                RatchetEngineAutoConfiguration.class,
                RatchetMicrometerAutoConfiguration.class))
        .withPropertyValues("ratchet.allowed-packages=example.jobs")
        .withBean(JobStore.class, () -> mock(JobStore.class))
        .withBean(RatchetRuntime.class, () -> mock(RatchetRuntime.class));
  }
}
