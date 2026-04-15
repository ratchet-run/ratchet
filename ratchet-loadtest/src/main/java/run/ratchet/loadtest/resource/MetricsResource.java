package run.ratchet.loadtest.resource;

import run.ratchet.loadtest.metrics.LoadTestMetricsBinder;
import run.ratchet.loadtest.metrics.PrometheusRegistryProducer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/metrics")
@ApplicationScoped
public class MetricsResource {

  @Inject PrometheusRegistryProducer registry;
  @Inject LoadTestMetricsBinder metricsBinder;

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String scrape() {
    metricsBinder.ensureBound();
    return registry.scrape();
  }
}
