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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.otel.OtelTracingCollector;
import run.ratchet.spi.TracingCollector;

/**
 * Verifies that {@link OtelTracingCollector} is selected over {@link
 * run.ratchet.ri.cdi.NoOpTracingCollector} when {@code ratchet-otel} is on the deployment
 * classpath.
 *
 * <p>{@code OtelTracingCollector} is annotated {@code @Alternative @Priority(1100)}, which globally
 * enables it per CDI 2.0+ spec without requiring a {@code beans.xml} entry. This test confirms the
 * CDI container honours the priority and selects the OTel implementation.
 *
 * <p>When no {@code OpenTelemetry} CDI bean is available (as in this test container), {@code
 * OtelTracingCollector} falls back to {@link io.opentelemetry.api.GlobalOpenTelemetry#get()}, which
 * returns a no-op instance. The test only verifies selection — not span emission.
 */
@ExtendWith(ArquillianExtension.class)
class RiOtelTracingAlternativeIT {

  @Inject private TracingCollector tracingCollector;

  @Deployment
  public static WebArchive createDeployment() {
    return RiOptionalModuleDeployment.create("run.ratchet:ratchet-otel");
  }

  @Test
  void otelAlternativeSelectedOverNoOp() {
    assertInstanceOf(
        OtelTracingCollector.class,
        tracingCollector,
        "When ratchet-otel is on the deployment classpath, the @Alternative @Priority(1100) "
            + "OtelTracingCollector must be selected over the default NoOpTracingCollector");
  }
}
