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
import run.ratchet.spi.MetricsCollector;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.jakarta.AbstractTxEnqueueContract;
import run.ratchet.tck.util.ConcurrentTestRunner;

/**
 * Verifies that {@link MicrometerMetricsCollector} is selected over {@link
 * run.ratchet.ri.cdi.NoOpMetricsCollector} when {@code ratchet-micrometer} is on the deployment
 * classpath.
 *
 * <p>{@code MicrometerMetricsCollector} is annotated {@code @Alternative @Priority(1000)}, which
 * globally enables it per CDI 2.0+ spec without requiring a {@code beans.xml} entry. This test
 * confirms that the CDI container honours the priority and selects the Micrometer implementation.
 * {@code ratchet-micrometer} also ships {@code MicrometerMeterRegistryProducer}, which provides the
 * {@code SimpleMeterRegistry} needed to satisfy {@code MicrometerMetricsCollector}'s constructor
 * injection — no additional test producer is required.
 */
@ExtendWith(ArquillianExtension.class)
class RiMicrometerAlternativeIT {

  @Inject private MetricsCollector metricsCollector;
  @Inject private MeterRegistry registry;

  @Deployment
  public static WebArchive createDeployment() {
    return RiOptionalModuleDeployment.create(
        "run.ratchet:ratchet-micrometer",
        new Package[] {
          RatchetTckRuntime.class.getPackage(),
          AbstractTxEnqueueContract.class.getPackage(),
          ConcurrentTestRunner.class.getPackage()
        },
        RiRatchetTckRuntime.class,
        ListenerProbe.class,
        TckJobs.class);
  }

  @Test
  void micrometerAlternativeSelectedOverNoOpAndRecordsMetrics() {
    assertInstanceOf(
        MicrometerMetricsCollector.class,
        metricsCollector,
        "When ratchet-micrometer is on the deployment classpath, the @Alternative @Priority(1000) "
            + "MicrometerMetricsCollector must be selected over the default NoOpMetricsCollector");

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
