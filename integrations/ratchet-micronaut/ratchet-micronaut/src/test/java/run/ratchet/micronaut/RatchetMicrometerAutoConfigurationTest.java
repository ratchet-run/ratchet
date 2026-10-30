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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micronaut.context.ApplicationContext;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.micrometer.MicrometerMetricTagPolicy;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.micrometer.MicrometerQueueHealthMetrics;
import run.ratchet.micrometer.MicrometerTracingCollector;
import run.ratchet.ri.cdi.NoOpTracingCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.spi.TracingCollector;

class RatchetMicrometerAutoConfigurationTest {
  @Test
  void registryEnablesMetricsAndQueueHealthHook() {
    try (var context = TestContexts.engine(Map.of())) {
      context.registerSingleton(MeterRegistry.class, new SimpleMeterRegistry());
      context.start();
      var collector = context.getBean(MetricsCollector.class);
      assertInstanceOf(MicrometerMetricsCollector.class, collector);
      assertEquals(1, context.getBeansOfType(MetricsCollector.class).size());
      collector.localWakeup("job_submit");
      var registry = context.getBean(MeterRegistry.class);
      assertEquals(
          1, registry.get("ratchet.wakeup.local").tag("source", "job_submit").counter().count());
      assertNull(registry.find("ratchet.queue.ready").gauge());
      var owner = new MicronautLifecycleHooks(context);
      var hooks = owner.acquire();
      assertTrue(hooks.contains(context.getBean(MicrometerQueueHealthMetrics.class)));
      try {
        hooks.forEach(SchedulerLifecycleHook::afterStart);
        assertNotNull(registry.find("ratchet.queue.ready").gauge());
      } finally {
        hooks.forEach(SchedulerLifecycleHook::beforeStop);
        hooks.forEach(owner::release);
      }
      assertNull(registry.find("ratchet.queue.ready").gauge());
    }
  }

  @Test
  void absentRegistryAndTracerKeepNoOps() {
    try (var context = TestContexts.core(Map.of())) {
      context.start();
      assertInstanceOf(NoOpMetricsCollector.class, context.getBean(MetricsCollector.class));
      assertInstanceOf(NoOpTracingCollector.class, context.getBean(TracingCollector.class));
      assertFalse(context.containsBean(MicrometerQueueHealthMetrics.class));
    }
  }

  @Test
  void userCollectorsAndQueueHealthOverrideDefaults() {
    var metrics = mock(MetricsCollector.class);
    var tracing = mock(TracingCollector.class);
    var health = mock(MicrometerQueueHealthMetrics.class);
    try (var context = TestContexts.core(Map.of())) {
      context.registerSingleton(MeterRegistry.class, new SimpleMeterRegistry());
      context.registerSingleton(Tracer.class, Tracer.NOOP);
      context.registerSingleton(MetricsCollector.class, metrics);
      context.registerSingleton(TracingCollector.class, tracing);
      context.registerSingleton(MicrometerQueueHealthMetrics.class, health);
      context.start();
      assertSame(metrics, context.getBean(MetricsCollector.class));
      assertSame(tracing, context.getBean(TracingCollector.class));
      assertSame(health, context.getBean(MicrometerQueueHealthMetrics.class));
      assertEquals(1, context.getBeansOfType(MetricsCollector.class).size());
      assertSame(metrics, context.getBeansOfType(MetricsCollector.class).iterator().next());
      assertEquals(1, context.getBeansOfType(TracingCollector.class).size());
      assertSame(tracing, context.getBeansOfType(TracingCollector.class).iterator().next());
      assertEquals(1, context.getBeansOfType(MicrometerQueueHealthMetrics.class).size());
      assertSame(
          health, context.getBeansOfType(MicrometerQueueHealthMetrics.class).iterator().next());
      var owner = new MicronautLifecycleHooks(context);
      var hooks = owner.acquire();
      try {
        assertEquals(List.of(health), hooks);
      } finally {
        hooks.forEach(owner::release);
      }
    }
  }

  @Test
  void customTagsExtendDefaults() {
    try (var context = TestContexts.core(Map.of())) {
      context.registerSingleton(MeterRegistry.class, new SimpleMeterRegistry());
      context.registerSingleton(
          MicrometerMetricTagPolicy.class,
          MicrometerMetricTagPolicy.builder().allowValue("source", "custom").build());
      context.start();
      var metrics = context.getBean(MetricsCollector.class);
      var registry = context.getBean(MeterRegistry.class);
      for (String source : new String[] {"custom", "job_submit", "unlisted"})
        metrics.localWakeup(source);
      for (String source : new String[] {"custom", "job_submit", "OTHER"})
        assertEquals(
            1, registry.get("ratchet.wakeup.local").tag("source", source).counter().count());
      context.getBean(MicrometerQueueHealthMetrics.class).afterStart();
      assertNull(registry.find("ratchet.queue.jobs").gauge());
    }
  }

  @Test
  void tracerWithoutRegistryEnablesTracingOnly() {
    try (var context = TestContexts.core(Map.of())) {
      context.registerSingleton(Tracer.class, Tracer.NOOP);
      context.start();
      assertInstanceOf(MicrometerTracingCollector.class, context.getBean(TracingCollector.class));
      assertEquals(1, context.getBeansOfType(TracingCollector.class).size());
      assertInstanceOf(NoOpMetricsCollector.class, context.getBean(MetricsCollector.class));
    }
  }

  @Test
  void disabledRatchetDisablesObservability() {
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false))) {
      context.registerSingleton(MeterRegistry.class, new SimpleMeterRegistry());
      context.registerSingleton(Tracer.class, Tracer.NOOP);
      context.start();
      assertFalse(context.containsBean(MetricsCollector.class));
      assertFalse(context.containsBean(TracingCollector.class));
      assertFalse(context.containsBean(MicrometerQueueHealthMetrics.class));
    }
  }

  @Test
  void missingOptionalClassesKeepFallbacks() {
    for (String hidden :
        new String[] {"io.micrometer.", "run.ratchet.micrometer.", "io.micrometer.tracing."}) {
      ClassLoader loader =
          new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              if (name.startsWith(hidden)) throw new ClassNotFoundException(name);
              return super.loadClass(name, resolve);
            }
          };
      try (var context =
          ApplicationContext.builder()
              .classLoader(loader)
              .deduceEnvironment(false)
              .enableDefaultPropertySources(false)
              .properties(Map.of("ratchet.allowed-packages", "example.jobs"))
              .build()) {
        context.start();
        assertInstanceOf(NoOpMetricsCollector.class, context.getBean(MetricsCollector.class));
        assertInstanceOf(NoOpTracingCollector.class, context.getBean(TracingCollector.class));
      }
    }
  }
}
