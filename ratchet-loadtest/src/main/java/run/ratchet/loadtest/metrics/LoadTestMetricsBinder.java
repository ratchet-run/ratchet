package run.ratchet.loadtest.metrics;

import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.management.ManagementFactory;
import java.time.Instant;

@ApplicationScoped
public class LoadTestMetricsBinder {

  @Inject JobCrudStore jobStore;
  @Inject PrometheusRegistryProducer prometheusRegistry;

  private boolean bound;

  public synchronized void ensureBound() {
    if (bound) {
      return;
    }
    MeterRegistry registry = prometheusRegistry.meterRegistry();

    Gauge.builder("ratchet.store.nodes.active", jobStore, JobCrudStore::countActiveNodes)
        .description("Registered Ratchet scheduler nodes")
        .register(registry);

    Gauge.builder(
            "ratchet.node.start.time.seconds",
            ManagementFactory.getRuntimeMXBean(),
            bean -> bean.getStartTime() / 1000.0)
        .description("JVM start time for this Ratchet node")
        .register(registry);

    Gauge.builder(
            "ratchet.store.jobs.ready", jobStore, store -> store.countReadyJobs(Instant.now()))
        .description("Jobs ready to be claimed")
        .register(registry);

    for (JobStatus status : JobStatus.values()) {
      Gauge.builder("ratchet.store.jobs", jobStore, store -> store.countJobsByStatus(status))
          .tag("status", status.name())
          .description("Jobs by persisted status")
          .register(registry);
    }

    Gauge.builder(
            "ratchet.signal.waiting_count",
            jobStore,
            store -> store.countJobsByStatus(JobStatus.WAITING))
        .description("Signal-waiting jobs currently blocked on an external signal")
        .register(registry);

    bound = true;
  }

  @PostConstruct
  void bind() {
    ensureBound();
  }
}
