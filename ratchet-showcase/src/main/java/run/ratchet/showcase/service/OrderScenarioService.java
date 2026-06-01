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
package run.ratchet.showcase.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.showcase.domain.GeneratedOrder;
import run.ratchet.showcase.domain.OrderProjection;
import run.ratchet.showcase.jobs.ShowcaseJobs;

@ApplicationScoped
public class OrderScenarioService {

  static final int DEMO_PAYMENT_OUTAGE_SECONDS = 5;

  @Inject JobSchedulerService scheduler;
  @Inject OrderRepository repository;
  @Inject SeededOrderGenerator generator;
  @Inject OrderWorkflowService workflow;

  public JobHandle submitGeneratedOrder(GeneratedOrder generated, String sourceTag) {
    OrderProjection order = repository.put(generated);
    String orderId = order.orderId;
    String effectiveSourceTag =
        sourceTag == null || sourceTag.isBlank() ? ShowcaseTags.ORDER : sourceTag;
    JobPriority priority = order.vip ? JobPriority.HIGH : JobPriority.NORMAL;
    JobHandle handle =
        scheduler
            .enqueue(() -> ShowcaseJobs.validateOrder(orderId))
            .withBusinessKey("showcase-order-" + orderId)
            .withTags(
                ShowcaseTags.SHOWCASE,
                ShowcaseTags.ORDER,
                effectiveSourceTag,
                ShowcaseTags.order(orderId))
            .withPriority(priority)
            .withResource("warehouse-robots")
            .withMaxRetries(2)
            .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
            .then(() -> ShowcaseJobs.scoreFraud(orderId))
            .then(() -> ShowcaseJobs.routeAfterFraud(orderId))
            .submit();
    repository.attachJob(orderId, handle.id().toString());
    return handle;
  }

  public JobHandle submitExistingOrder(String orderId) {
    OrderProjection order = repository.require(orderId);
    return submitGeneratedOrder(toGenerated(order), ShowcaseTags.BURST);
  }

  public JobHandle importBurst(Integer count, Long seed) {
    int size = count == null ? 25 : Math.max(1, Math.min(200, count));
    long effectiveSeed = seed == null ? repository.streamState().seed : seed;
    List<String> orderIds = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      long sequence = repository.nextSequence();
      GeneratedOrder order =
          generator.generate(effectiveSeed, sequence, repository.streamState().failureMix);
      repository.put(order);
      orderIds.add(order.orderId());
    }
    return scheduler
        .enqueueBatch("showcase-burst-import")
        .forEach(orderIds, ShowcaseJobs::startImportedOrder)
        .onProgress(ShowcaseJobs::recordBurstProgress)
        .thenOnBatchSuccess(ShowcaseJobs::reconcileOrders)
        .submit();
  }

  public JobHandle fraudReview() {
    return submitGeneratedOrder(
        forcedOrder(
            "Summit Outfitters",
            "SKU-LEDGER-PRO",
            2,
            "RNO-4",
            true,
            92,
            "NORMAL",
            "UPS",
            false,
            false),
        ShowcaseTags.SCENARIO_FRAUD);
  }

  public JobHandle badCard() {
    return submitGeneratedOrder(
        forcedOrder(
            "Lakefront Supply",
            "SKU-SENSOR-KIT",
            1,
            "DFW-2",
            false,
            34,
            "BAD_CARD",
            "FedEx",
            false,
            false),
        ShowcaseTags.SCENARIO_BAD_CARD);
  }

  public List<JobHandle> warehouseCrunch(Integer count) {
    int size = count == null ? 16 : Math.max(1, Math.min(50, count));
    List<JobHandle> handles = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      handles.add(
          submitGeneratedOrder(
              forcedOrder(
                  "Cobalt Books",
                  "SKU-ROBOT-ARM",
                  3 + i % 4,
                  i % 2 == 0 ? "PHX-1" : "DFW-2",
                  i % 5 == 0,
                  38 + i % 12,
                  "NORMAL",
                  "UPS",
                  false,
                  true),
              ShowcaseTags.SCENARIO_WAREHOUSE));
    }
    return handles;
  }

  public List<JobHandle> carrierOutage(Integer count) {
    int size = count == null ? 8 : Math.max(1, Math.min(40, count));
    List<JobHandle> handles = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      handles.add(
          submitGeneratedOrder(
              forcedOrder(
                  "Meridian Health",
                  "SKU-COLD-PACK",
                  1 + i % 3,
                  "ABE-3",
                  false,
                  29 + i % 18,
                  "NORMAL",
                  "Regional Courier",
                  false,
                  false),
              ShowcaseTags.SCENARIO_CARRIER));
    }
    return handles;
  }

  public List<JobHandle> paymentOutageTraffic(Integer count) {
    int size = count == null ? 8 : Math.max(1, Math.min(40, count));
    List<JobHandle> handles = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      handles.add(
          submitGeneratedOrder(
              forcedOrder(
                  "Signal Labs",
                  "SKU-DRONE-BAY",
                  2 + i % 4,
                  "RNO-4",
                  i % 4 == 0,
                  31 + i % 16,
                  "NORMAL",
                  "DHL",
                  false,
                  false),
              ShowcaseTags.SCENARIO_PAYMENT));
    }
    return handles;
  }

  public Instant paymentOutage(Integer seconds) {
    return workflow.startPaymentOutage(seconds == null ? DEMO_PAYMENT_OUTAGE_SECONDS : seconds);
  }

  public void recordBurstProgress(run.ratchet.api.BatchContext context) {
    repository.recordBurstProgress(context);
  }

  private static GeneratedOrder toGenerated(OrderProjection order) {
    return new GeneratedOrder(
        order.orderId,
        order.sequence,
        order.createdAt,
        order.customer,
        order.sku,
        order.quantity,
        order.warehouse,
        order.vip,
        order.fraudScore,
        order.paymentProfile,
        order.carrier,
        order.addressBad,
        order.inventoryPressure);
  }

  private GeneratedOrder forcedOrder(
      String customer,
      String sku,
      int quantity,
      String warehouse,
      boolean vip,
      int fraudScore,
      String paymentProfile,
      String carrier,
      boolean addressBad,
      boolean inventoryPressure) {
    long sequence = repository.nextSequence();
    return new GeneratedOrder(
        "ORD-" + String.format("%06d", sequence),
        sequence,
        Instant.EPOCH.plusSeconds(sequence),
        customer,
        sku,
        quantity,
        warehouse,
        vip,
        fraudScore,
        paymentProfile,
        carrier,
        addressBad,
        inventoryPressure);
  }
}
