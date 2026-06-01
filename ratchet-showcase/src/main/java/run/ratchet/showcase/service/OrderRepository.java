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
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import run.ratchet.api.BatchContext;
import run.ratchet.showcase.domain.GeneratedOrder;
import run.ratchet.showcase.domain.OrderProjection;
import run.ratchet.showcase.domain.OrderStatus;
import run.ratchet.showcase.domain.ReviewTicket;
import run.ratchet.showcase.domain.StreamState;

@ApplicationScoped
public class OrderRepository {

  private final Map<String, OrderProjection> orders = new ConcurrentHashMap<>();
  private final Map<String, ReviewTicket> reviews = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> attempts = new ConcurrentHashMap<>();
  private final AtomicLong sequence = new AtomicLong();
  private final StreamState streamState = StreamState.stopped(SeededOrderGenerator.DEFAULT_SEED);
  private volatile Map<String, Object> burstProgress = Map.of();

  public StreamState streamState() {
    return streamState.copy();
  }

  public void startStream(long seed, int ordersPerMinute, double burstiness, double failureMix) {
    streamState.start(seed, ordersPerMinute, burstiness, failureMix);
  }

  public void updateStream(
      Integer ordersPerMinute, Long seed, Double burstiness, Double failureMix) {
    streamState.update(ordersPerMinute, seed, burstiness, failureMix);
  }

  public void stopStream() {
    streamState.stop();
  }

  public int drainDueOrders() {
    return streamState.drainDueOrders(Instant.now());
  }

  public long nextSequence() {
    return sequence.incrementAndGet();
  }

  public OrderProjection put(GeneratedOrder generated) {
    OrderProjection projection = OrderProjection.from(generated);
    orders.put(projection.orderId, projection);
    return projection;
  }

  public Optional<OrderProjection> find(String orderId) {
    return Optional.ofNullable(orders.get(orderId));
  }

  public OrderProjection require(String orderId) {
    OrderProjection order = orders.get(orderId);
    if (order == null) {
      throw new IllegalArgumentException("Unknown showcase order " + orderId);
    }
    return order;
  }

  public void transition(String orderId, OrderStatus status, String message) {
    require(orderId).transition(status, message);
  }

  public void attachJob(String orderId, String jobId) {
    require(orderId).attachJob(jobId);
  }

  public long incrementAttempt(String orderId, String step) {
    return attempts
        .computeIfAbsent(orderId + ":" + step, ignored -> new AtomicLong())
        .incrementAndGet();
  }

  public ReviewTicket openReview(String orderId) {
    OrderProjection order = require(orderId);
    String reviewId = "review-" + orderId;
    return reviews.computeIfAbsent(reviewId, ignored -> new ReviewTicket(reviewId, order));
  }

  public Optional<ReviewTicket> review(String reviewId) {
    return Optional.ofNullable(reviews.get(reviewId));
  }

  public List<ReviewTicket> openReviews() {
    return reviews.values().stream()
        .filter(ReviewTicket::isOpen)
        .map(ReviewTicket::snapshot)
        .sorted(
            Comparator.comparing((ReviewTicket review) -> review.vip)
                .reversed()
                .thenComparing(review -> review.createdAt)
                .thenComparing(review -> review.id))
        .limit(25)
        .toList();
  }

  public void decideReview(String reviewId, String decision, String reason) {
    ReviewTicket review = reviews.get(reviewId);
    if (review != null) {
      review.decide(decision, reason);
    }
  }

  public List<OrderProjection> recentOrders(int limit) {
    return orders.values().stream()
        .map(OrderProjection::snapshot)
        .sorted(Comparator.comparing((OrderProjection order) -> order.updatedAt).reversed())
        .limit(limit)
        .toList();
  }

  public Map<OrderStatus, Long> statusCounts() {
    Map<OrderStatus, Long> counts = new EnumMap<>(OrderStatus.class);
    for (OrderStatus status : OrderStatus.values()) {
      counts.put(status, 0L);
    }
    for (OrderProjection order : orders.values()) {
      counts.merge(order.status(), 1L, Long::sum);
    }
    return counts;
  }

  public void reconcileRecentFulfilledOrders(int limit) {
    orders.values().stream()
        .sorted(Comparator.comparingLong((OrderProjection order) -> order.sequence).reversed())
        .limit(limit)
        .filter(order -> order.status() == OrderStatus.RECEIPT_SENT)
        .forEach(
            order ->
                order.transition(OrderStatus.RECEIPT_SENT, "Reconciled with fulfillment ledger"));
  }

  public long totalOrders() {
    return orders.size();
  }

  public long openReviewCount() {
    return reviews.values().stream().filter(review -> "OPEN".equals(review.status)).count();
  }

  public void recordBurstProgress(BatchContext context) {
    burstProgress =
        Map.of(
            "batchId", context.batchId().toString(),
            "totalItems", context.totalItems(),
            "completedItems", context.completedItems(),
            "failedItems", context.failedItems(),
            "percentDone", context.percentDone(),
            "complete", context.isComplete());
  }

  public Map<String, Object> burstProgress() {
    return burstProgress;
  }

  public void reset() {
    orders.clear();
    reviews.clear();
    attempts.clear();
    sequence.set(0);
    streamState.stop();
    burstProgress = Map.of();
  }

  public List<ReviewTicket> allReviews() {
    return reviews.values().stream().map(ReviewTicket::snapshot).toList();
  }
}
