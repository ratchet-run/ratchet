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
import java.util.List;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;

@ApplicationScoped
public class ShowcaseCircuitBreakers {

  private static final List<String> JOB_SERVICES =
      List.of(
          "ShowcaseJobs.produceOrderStream",
          "ShowcaseJobs.startImportedOrder",
          "ShowcaseJobs.recordBurstProgress",
          "ShowcaseJobs.validateOrder",
          "ShowcaseJobs.scoreFraud",
          "ShowcaseJobs.routeAfterFraud",
          "ShowcaseJobs.startFulfillment",
          "ShowcaseJobs.reserveInventory",
          "ShowcaseJobs.chargePayment",
          "ShowcaseJobs.createShipment",
          "ShowcaseJobs.sendReceipt",
          "ShowcaseJobs.applyReviewDecision",
          "ShowcaseJobs.markOrderFailed",
          "ShowcaseJobs.reconcileOrders");

  @Inject CircuitBreakerRegistry circuitBreakers;

  public void resetAll() {
    JOB_SERVICES.forEach(circuitBreakers::resetBreaker);
  }
}
