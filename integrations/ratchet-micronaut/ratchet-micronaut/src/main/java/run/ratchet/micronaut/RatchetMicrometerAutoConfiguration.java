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
import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import run.ratchet.api.JobQueryService;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.micrometer.MicrometerQueueHealthMetrics;

/** Registers queue-health metrics for application-provided Micrometer registries. */
@Factory
@Requires(classes = MicrometerMetricsCollector.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
public class RatchetMicrometerAutoConfiguration {
  @Factory
  @Requires(
      classes = {MeterRegistry.class, MicrometerMetricsCollector.class},
      beans = MeterRegistry.class)
  @Requires(property = "ratchet.enabled", notEquals = "false")
  static class MetricsConfiguration {
    @Singleton
    @Requires(missingBeans = MicrometerQueueHealthMetrics.class)
    MicrometerQueueHealthMetrics ratchetQueueHealthMetrics(
        MeterRegistry registry, BeanProvider<JobQueryService> queries) {
      return new MicrometerQueueHealthMetrics(registry, queries.orElse(null));
    }
  }
}
