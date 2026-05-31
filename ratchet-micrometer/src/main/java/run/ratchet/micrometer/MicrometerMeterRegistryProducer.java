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
package run.ratchet.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Default {@link MeterRegistry} producer for the {@code ratchet-micrometer} module.
 *
 * <p>Provides a {@link SimpleMeterRegistry} when the application has not declared its own {@code
 * MeterRegistry} bean. This is the "drop-in" experience: add {@code ratchet-micrometer} to the
 * classpath and metrics start appearing immediately, no extra wiring required.
 *
 * <p>Production deployments should override this with their own {@code @Alternative @Priority}
 * producer that wires a real registry (Prometheus, Datadog, OpenTelemetry, etc.). Example:
 *
 * <pre>{@code
 * @Produces
 * @Alternative
 * @Priority(2000)
 * @ApplicationScoped
 * public MeterRegistry prometheusRegistry() {
 *   return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
 * }
 * }</pre>
 */
@ApplicationScoped
public class MicrometerMeterRegistryProducer {

  private final MeterRegistry defaultRegistry = new SimpleMeterRegistry();

  // Producer scope is @Singleton rather than @ApplicationScoped so Weld doesn't need
  // a proxy on MeterRegistry — the abstract base class has no no-args constructor
  // (WELD-001435). @Singleton still hands out the same instance to every injection point.
  @Produces
  @Default
  @Singleton
  public MeterRegistry defaultRegistry() {
    return defaultRegistry;
  }
}
