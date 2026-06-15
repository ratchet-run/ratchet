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
package run.ratchet.showcase.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.SignalDecision;
import run.ratchet.showcase.domain.GeneratedOrder;
import run.ratchet.showcase.domain.OrderStatus;
import run.ratchet.showcase.service.OrderRepository;
import run.ratchet.showcase.service.OrderWorkflowService;

class ShowcaseResourceTest {

  @Test
  void signalDeliveryClosesReviewWithoutManualFulfillment() {
    Fixture fixture = fixture("ORD-000001");
    when(fixture.scheduler.deliverSignal(eq("review-ORD-000001"), any(SignalDecision.class)))
        .thenReturn(1);

    Map<String, Object> response =
        fixture.resource.reviewDecision("review-ORD-000001", decision("approve", null));

    assertEquals(1, response.get("delivered"));
    assertEquals("signal", response.get("handled"));
    assertTrue(fixture.repository.openReviews().isEmpty());
    verifyNoInteractions(fixture.workflow);
  }

  @Test
  void zeroDeliveredApprovalCompletesStaleReviewAndStartsFulfillment() {
    Fixture fixture = fixture("ORD-000002");
    when(fixture.scheduler.deliverSignal(eq("review-ORD-000002"), any(SignalDecision.class)))
        .thenReturn(0);

    Map<String, Object> response =
        fixture.resource.reviewDecision("review-ORD-000002", decision("approve", null));

    assertEquals(0, response.get("delivered"));
    assertEquals("manual", response.get("handled"));
    assertTrue(fixture.repository.openReviews().isEmpty());
    assertEquals(OrderStatus.REVIEW_APPROVED, fixture.repository.require("ORD-000002").status());
    verify(fixture.workflow).startFulfillment("ORD-000002");
  }

  @Test
  void zeroDeliveredRejectionCompletesStaleReviewWithoutFulfillment() {
    Fixture fixture = fixture("ORD-000003");
    when(fixture.scheduler.deliverSignal(eq("review-ORD-000003"), any(SignalDecision.class)))
        .thenReturn(0);

    Map<String, Object> response =
        fixture.resource.reviewDecision(
            "review-ORD-000003", decision("reject", "Dashboard rejected"));

    assertEquals(0, response.get("delivered"));
    assertEquals("manual", response.get("handled"));
    assertTrue(fixture.repository.openReviews().isEmpty());
    assertEquals(OrderStatus.REVIEW_REJECTED, fixture.repository.require("ORD-000003").status());
    verifyNoInteractions(fixture.workflow);
  }

  private static Fixture fixture(String orderId) {
    OrderRepository repository = new OrderRepository();
    repository.put(
        new GeneratedOrder(
            orderId,
            1L,
            Instant.parse("2026-06-01T00:00:00Z"),
            "Acme Field Services",
            "SKU-ROBOT-ARM",
            2,
            "PHX-1",
            true,
            88,
            "NORMAL",
            "UPS",
            false,
            false));
    repository.transition(orderId, OrderStatus.REVIEW_REQUIRED, "Waiting for fraud review");
    repository.openReview(orderId);

    ShowcaseResource resource = new ShowcaseResource();
    resource.repository = repository;
    resource.scheduler = mock(JobSchedulerService.class);
    resource.workflow = mock(OrderWorkflowService.class);
    return new Fixture(resource, repository, resource.scheduler, resource.workflow);
  }

  private static DecisionRequest decision(String decision, String reason) {
    DecisionRequest request = new DecisionRequest();
    request.decision = decision;
    request.reason = reason;
    return request;
  }

  private record Fixture(
      ShowcaseResource resource,
      OrderRepository repository,
      JobSchedulerService scheduler,
      OrderWorkflowService workflow) {}
}
