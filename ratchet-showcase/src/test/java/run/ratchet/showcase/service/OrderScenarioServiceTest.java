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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.showcase.domain.OrderProjection;

class OrderScenarioServiceTest {

  @Test
  void defaultPaymentOutageFitsNormalRetryWindow() {
    OrderScenarioService scenarios = new OrderScenarioService();
    scenarios.workflow = new OrderWorkflowService();

    Instant before = Instant.now();
    Instant until = scenarios.paymentOutage(null);

    assertFalse(
        until.isBefore(before.plusSeconds(OrderScenarioService.DEMO_PAYMENT_OUTAGE_SECONDS)));
    assertFalse(
        until.isAfter(before.plusSeconds(OrderScenarioService.DEMO_PAYMENT_OUTAGE_SECONDS + 1L)));
  }

  @Test
  void fraudReviewScenarioCreatesHighRiskOrder() {
    Fixture fixture = fixture();

    fixture.scenarios.fraudReview();

    OrderProjection order = fixture.repository.recentOrders(1).get(0);
    assertTrue(order.fraudScore >= 70);
    assertEquals("NORMAL", order.paymentProfile);
    assertFalse(order.addressBad);
  }

  @Test
  void badCardScenarioCreatesPermanentPaymentFailureCandidate() {
    Fixture fixture = fixture();

    fixture.scenarios.badCard();

    OrderProjection order = fixture.repository.recentOrders(1).get(0);
    assertEquals("BAD_CARD", order.paymentProfile);
    assertTrue(order.fraudScore < 70);
    assertFalse(order.addressBad);
  }

  @Test
  void warehouseCrunchCreatesInventoryPressureOrders() {
    Fixture fixture = fixture();

    fixture.scenarios.warehouseCrunch(5);

    assertEquals(5, fixture.repository.totalOrders());
    assertTrue(
        fixture.repository.recentOrders(5).stream().allMatch(order -> order.inventoryPressure));
  }

  @Test
  void carrierOutageCreatesRegionalCourierOrders() {
    Fixture fixture = fixture();

    fixture.scenarios.carrierOutage(4);

    assertEquals(4, fixture.repository.totalOrders());
    assertTrue(
        fixture.repository.recentOrders(4).stream()
            .allMatch(order -> "Regional Courier".equals(order.carrier)));
  }

  private static Fixture fixture() {
    OrderRepository repository = new OrderRepository();
    JobSchedulerService scheduler = mock(JobSchedulerService.class);
    JobBuilder builder = mock(JobBuilder.class, Answers.RETURNS_SELF);
    when(scheduler.enqueue(any())).thenReturn(builder);
    when(builder.submit())
        .thenReturn(() -> UUID.fromString("019e847d-abf9-7000-aa44-c7a749424220"));

    OrderScenarioService scenarios = new OrderScenarioService();
    scenarios.scheduler = scheduler;
    scenarios.repository = repository;
    scenarios.generator = new SeededOrderGenerator();
    return new Fixture(scenarios, repository);
  }

  private record Fixture(OrderScenarioService scenarios, OrderRepository repository) {}
}
