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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import io.micronaut.context.BeanContext;
import run.ratchet.micrometer.MicrometerMetricTagPolicy;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.micrometer.MicrometerTracingCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;

/** Loaded only after the corresponding optional observability types and bean are available. */
final class MicrometerCollectors {
  private MicrometerCollectors() {}

  static MetricsCollector metrics(BeanContext context) {
    var registry = context.getBean(MeterRegistry.class);
    var policy = context.findBean(MicrometerMetricTagPolicy.class).orElse(null);
    return policy == null
        ? new MicrometerMetricsCollector(registry)
        : new MicrometerMetricsCollector(registry, policy);
  }

  static TracingCollector tracing(BeanContext context) {
    return new MicrometerTracingCollector(
        context.getBean(Tracer.class), context.findBean(Propagator.class).orElse(null));
  }
}
