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
package run.ratchet.showcase.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import run.ratchet.api.JobStatus;
import run.ratchet.showcase.service.OrderRepository;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobCrudStore;

@ApplicationScoped
public class ShowcaseMetricsBinder {

  @Inject JobCrudStore jobStore;
  @Inject JobAnalyticsStore analyticsStore;
  @Inject MeterRegistry registry;
  @Inject OrderRepository orders;

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

  @PostConstruct
  void bind() {
    ensureBound();
  }

  private void bindGauges() {
    Gauge.builder("ratchet.showcase.orders.total", orders, OrderRepository::totalOrders)
        .description("In-memory showcase order count")
        .register(registry);
    Gauge.builder("ratchet.showcase.reviews.open", orders, OrderRepository::openReviewCount)
        .description("Open showcase review tickets")
        .register(registry);
    Gauge.builder("ratchet.store.nodes.active", jobStore, JobCrudStore::countActiveNodes)
        .description("Registered Ratchet scheduler nodes")
        .register(registry);
    for (JobStatus status : JobStatus.values()) {
      Gauge.builder("ratchet.store.jobs", analyticsStore, store -> store.countJobsByStatus(status))
          .tag("status", status.name())
          .description("Jobs by persisted status")
          .register(registry);
    }
  }
}
