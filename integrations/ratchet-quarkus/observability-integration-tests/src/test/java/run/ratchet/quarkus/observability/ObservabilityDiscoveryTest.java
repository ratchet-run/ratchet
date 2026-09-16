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

import io.quarkus.arc.Arc;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.otel.OtelTracingCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;

/** A consumer of the observability dependencies, without manual bean registration or indexing. */
@QuarkusTest
class ObservabilityDiscoveryTest {
  @jakarta.inject.Inject TracingCollector tracing;
  @jakarta.inject.Inject MetricsCollector metrics;

  @Test
  void packagedAdaptersAreDiscoveredAndOtelWinsTracing() {
    assertEquals(
        OtelTracingCollector.class,
        Arc.container().instance(TracingCollector.class).getBean().getBeanClass());
    assertEquals(
        MicrometerMetricsCollector.class,
        Arc.container().instance(MetricsCollector.class).getBean().getBeanClass());
  }
}
