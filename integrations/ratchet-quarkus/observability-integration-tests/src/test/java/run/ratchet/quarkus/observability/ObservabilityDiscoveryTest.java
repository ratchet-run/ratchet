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
package run.ratchet.quarkus.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Arc;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.micrometer.MicrometerQueueHealthMetrics;
import run.ratchet.otel.OtelTracingCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.spi.TracingCollector;

/** A consumer of the observability dependencies, without manual bean registration or indexing. */
@QuarkusTest
class ObservabilityDiscoveryTest {
  @Inject TracingCollector tracing;
  @Inject MetricsCollector metrics;

  // Same lookup as DefaultRatchetLifecycle; an Instance injection point keeps ArC from removing
  // the hook beans.
  @Inject Instance<SchedulerLifecycleHook> lifecycleHooks;

  @Test
  void packagedAdaptersAreDiscoveredAndOtelWinsTracing() {
    assertEquals(
        OtelTracingCollector.class,
        Arc.container().instance(TracingCollector.class).getBean().getBeanClass());
    assertEquals(
        MicrometerMetricsCollector.class,
        Arc.container().instance(MetricsCollector.class).getBean().getBeanClass());
  }

  @Test
  void queueHealthHookIsDiscoveredAndRegistersGauges() {
    SchedulerLifecycleHook hook =
        lifecycleHooks.stream()
            .filter(MicrometerQueueHealthMetrics.class::isInstance)
            .findFirst()
            .orElseThrow();
    MeterRegistry registry = Arc.container().instance(MeterRegistry.class).get();

    hook.afterStart();
    try {
      assertEquals(
          7.0, registry.get("ratchet.queue.jobs").tag("status", "PENDING").gauge().value());
    } finally {
      hook.beforeStop();
    }
    assertNull(registry.find("ratchet.queue.jobs").gauge());
  }
}
