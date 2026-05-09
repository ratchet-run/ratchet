package run.ratchet.loadtest.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Specializes;
import run.ratchet.micrometer.MicrometerMeterRegistryProducer;

@ApplicationScoped
@Specializes
public class PrometheusRegistryProducer extends MicrometerMeterRegistryProducer {

  private final PrometheusMeterRegistry registry;

  public PrometheusRegistryProducer() {
    this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    this.registry.config().meterFilter(new JobDurationHistogramFilter());
  }

  @Produces
  @Default
  @Dependent
  @Override
  public MeterRegistry defaultRegistry() {
    return registry;
  }

  public String scrape() {
    return registry.scrape();
  }

  private static final class JobDurationHistogramFilter implements MeterFilter {

    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
      if (!"ratchet.jobs.duration".equals(id.getName())) {
        return config;
      }
      return DistributionStatisticConfig.builder().percentilesHistogram(true).build().merge(config);
    }
  }
}
