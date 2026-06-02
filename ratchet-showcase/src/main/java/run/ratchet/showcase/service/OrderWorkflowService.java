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

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.SignalDecision;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.showcase.domain.OrderProjection;
import run.ratchet.showcase.domain.OrderStatus;
import run.ratchet.showcase.domain.ReviewTicket;
import run.ratchet.showcase.jobs.ShowcaseJobs;

@ApplicationScoped
public class OrderWorkflowService {

  static final Duration REVIEW_SIGNAL_TIMEOUT = Duration.ofHours(4);

  @Inject OrderRepository repository;
  @Inject JobSchedulerService scheduler;
  @Inject ResourcePermitService resources;

  private volatile Instant paymentOutageUntil = Instant.EPOCH;

  @PostConstruct
  void configureResources() {
    resources.configureResource("payment-gateway", 3, 1500, "External payment gateway");
    resources.configureResource("warehouse-robots", 4, 1000, "Warehouse robot picks");
    resources.configureResource("carrier-api", 2, 2000, "Carrier label API");
  }

  public void validateOrder(String orderId) {
    OrderProjection order = repository.require(orderId);
    if (order.addressBad) {
      repository.transition(orderId, OrderStatus.FAILED, "Address validation failed permanently");
      throw new PermanentShowcaseFailure("Bad address for " + orderId);
    }
    repository.transition(orderId, OrderStatus.VALIDATED, "Customer and address validated");
  }

  public void scoreFraud(String orderId) {
    OrderProjection order = repository.require(orderId);
    repository.transition(orderId, OrderStatus.FRAUD_SCORED, "Fraud score " + order.fraudScore);
  }

  public void routeAfterFraud(String orderId) {
    OrderProjection order = repository.require(orderId);
    if (order.fraudScore >= 70) {
      ReviewTicket review = repository.openReview(orderId);
      repository.transition(orderId, OrderStatus.REVIEW_REQUIRED, "Waiting for fraud review");
      scheduler
          .enqueue(() -> ShowcaseJobs.applyReviewDecision(orderId))
          .awaitSignal(review.id, REVIEW_SIGNAL_TIMEOUT)
          .withBusinessKey(review.id)
          .withTags(
              ShowcaseTags.SHOWCASE,
              ShowcaseTags.REVIEW,
              ShowcaseTags.ORDER,
              ShowcaseTags.order(orderId))
          .withPriority(order.vip ? JobPriority.CRITICAL : JobPriority.HIGH)
          .withMaxRetries(0)
          .thenOnSuccess(() -> ShowcaseJobs.startFulfillment(orderId))
          .submit();
      return;
    }
    startFulfillment(orderId);
  }

  public void startFulfillment(String orderId) {
    OrderProjection order = repository.require(orderId);
    scheduler
        .enqueue(() -> ShowcaseJobs.reserveInventory(orderId))
        .withBusinessKey("showcase-fulfillment-" + orderId)
        .withTags(ShowcaseTags.SHOWCASE, ShowcaseTags.ORDER, ShowcaseTags.order(orderId))
        .withPriority(order.vip ? JobPriority.HIGH : JobPriority.NORMAL)
        .withResource("warehouse-robots")
        .withMaxRetries(2)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
        .then(() -> ShowcaseJobs.chargePayment(orderId))
        .then(() -> ShowcaseJobs.createShipment(orderId))
        .then(() -> ShowcaseJobs.sendReceipt(orderId))
        .submit();
  }

  public void reserveInventory(String orderId) {
    OrderProjection order = repository.require(orderId);
    long attempt = repository.incrementAttempt(orderId, "inventory");
    if (order.inventoryPressure && attempt == 1) {
      repository.transition(orderId, OrderStatus.FRAUD_SCORED, "Inventory pressure, retrying pick");
      throw new TransientShowcaseFailure("Inventory pick contention for " + orderId);
    }
    repository.transition(
        orderId, OrderStatus.INVENTORY_RESERVED, "Inventory reserved in " + order.warehouse);
  }

  public void chargePayment(String orderId) {
    OrderProjection order = repository.require(orderId);
    long attempt = repository.incrementAttempt(orderId, "payment");
    if (Instant.now().isBefore(paymentOutageUntil)) {
      repository.transition(orderId, OrderStatus.INVENTORY_RESERVED, "Payment outage, backing off");
      throw new TransientShowcaseFailure("Payment gateway outage");
    }
    if ("BAD_CARD".equals(order.paymentProfile)) {
      repository.transition(orderId, OrderStatus.FAILED, "Bad card moved to DLQ");
      throw new PermanentShowcaseFailure("Permanent payment failure for " + orderId);
    }
    if ("TRANSIENT_GATEWAY".equals(order.paymentProfile) && attempt < 3) {
      repository.transition(
          orderId, OrderStatus.INVENTORY_RESERVED, "Gateway timeout, retrying payment");
      throw new TransientShowcaseFailure("Transient payment timeout for " + orderId);
    }
    repository.transition(orderId, OrderStatus.PAYMENT_CHARGED, "Payment authorized");
  }

  public void createShipment(String orderId) {
    OrderProjection order = repository.require(orderId);
    long attempt = repository.incrementAttempt(orderId, "shipment");
    if ("Regional Courier".equals(order.carrier) && attempt == 1) {
      repository.transition(orderId, OrderStatus.PAYMENT_CHARGED, "Carrier capacity retry");
      throw new TransientShowcaseFailure("Carrier capacity retry for " + orderId);
    }
    repository.transition(
        orderId, OrderStatus.SHIPMENT_CREATED, "Shipment booked with " + order.carrier);
    scheduler
        .schedule(Duration.ofSeconds(45), () -> ShowcaseJobs.reconcileOrders())
        .withTags(ShowcaseTags.SHOWCASE, ShowcaseTags.SCENARIO)
        .withPriority(JobPriority.LOW)
        .submit();
  }

  public void sendReceipt(String orderId) {
    repository.transition(orderId, OrderStatus.RECEIPT_SENT, "Receipt sent and order complete");
  }

  public void applyReviewDecision(String orderId) {
    SignalDecision decision = JobContext.current().signalPayload(SignalDecision.class);
    ReviewTicket review = repository.openReview(orderId);
    if (decision == null || decision.isRejected()) {
      String reason = decision == null ? "No decision payload" : decision.rejectionReason();
      repository.decideReview(review.id, "REJECT", reason);
      repository.transition(orderId, OrderStatus.REVIEW_REJECTED, reason);
      throw new PermanentShowcaseFailure("Review rejected " + orderId + ": " + reason);
    }
    repository.decideReview(review.id, "APPROVE", "Approved in dashboard");
    repository.transition(orderId, OrderStatus.REVIEW_APPROVED, "Fraud review approved");
  }

  public void markOrderFailed(String orderId) {
    repository.transition(orderId, OrderStatus.FAILED, "Workflow failed");
  }

  public void reconcileOrders() {
    repository.reconcileRecentFulfilledOrders(50);
  }

  public Instant startPaymentOutage(int seconds) {
    paymentOutageUntil = Instant.now().plusSeconds(Math.max(1, Math.min(300, seconds)));
    return paymentOutageUntil;
  }

  public Instant paymentOutageUntil() {
    return paymentOutageUntil;
  }

  public static class TransientShowcaseFailure extends RuntimeException {
    public TransientShowcaseFailure(String message) {
      super(message);
    }
  }

  public static class PermanentShowcaseFailure extends RuntimeException {
    public PermanentShowcaseFailure(String message) {
      super(message);
    }
  }
}
