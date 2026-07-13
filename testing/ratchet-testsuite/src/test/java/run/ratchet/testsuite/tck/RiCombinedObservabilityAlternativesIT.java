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
package run.ratchet.testsuite.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.otel.OtelTracingCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;

/** Verifies deterministic adapter selection when both observability modules are installed. */
@ExtendWith(ArquillianExtension.class)
class RiCombinedObservabilityAlternativesIT {

  @Inject private TracingCollector tracingCollector;
  @Inject private MetricsCollector metricsCollector;
  @Inject private MeterRegistry registry;

  @Deployment
  public static WebArchive createDeployment() {
    return RiOptionalModuleDeployment.create(
        "run.ratchet:ratchet-otel", "run.ratchet:ratchet-micrometer");
  }

  @Test
  void otelTracingAndMicrometerMetricsAreSelectedTogether() {
    assertInstanceOf(OtelTracingCollector.class, tracingCollector);
    assertInstanceOf(MicrometerMetricsCollector.class, metricsCollector);

    metricsCollector.jobStarted(new UUID(0L, 1L), JobType.SINGLE, JobPriority.NORMAL);

    assertEquals(
        1.0,
        registry
            .get("ratchet.jobs.started")
            .tag("type", "SINGLE")
            .tag("priority", "NORMAL")
            .counter()
            .count());
  }
}
