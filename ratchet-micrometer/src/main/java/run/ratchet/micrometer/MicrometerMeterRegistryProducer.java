package run.ratchet.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;

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

  @Produces
  @Default
  @Dependent
  public MeterRegistry defaultRegistry() {
    return defaultRegistry;
  }
}
