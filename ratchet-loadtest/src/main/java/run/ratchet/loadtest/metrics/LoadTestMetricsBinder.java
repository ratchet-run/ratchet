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
package run.ratchet.loadtest.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;

@ApplicationScoped
public class LoadTestMetricsBinder {

  @Inject JobCrudStore jobStore;
  @Inject MeterRegistry registry;

  private volatile boolean bound;

  public void ensureBound() {
    if (bound) {
      return;
    }
    synchronized (this) {
      if (bound) {
        return;
      }
      bindGauges();
      bound = true;
    }
  }

  private void bindGauges() {
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
  }

  @PostConstruct
  void bind() {
    ensureBound();
  }
}
