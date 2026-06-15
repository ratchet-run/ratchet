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
package run.ratchet.showcase.jobs;

import jakarta.enterprise.inject.spi.CDI;
import run.ratchet.api.BatchContext;
import run.ratchet.showcase.service.OrderScenarioService;
import run.ratchet.showcase.service.OrderStreamService;
import run.ratchet.showcase.service.OrderWorkflowService;

public final class ShowcaseJobs {

  private ShowcaseJobs() {}

  public static void produceOrderStream() {
    stream().produceDueOrders();
  }

  public static void startImportedOrder(String orderId) {
    scenarios().submitExistingOrder(orderId);
  }

  public static void recordBurstProgress(BatchContext context) {
    scenarios().recordBurstProgress(context);
  }

  public static void validateOrder(String orderId) {
    workflow().validateOrder(orderId);
  }

  public static void scoreFraud(String orderId) {
    workflow().scoreFraud(orderId);
  }

  public static void routeAfterFraud(String orderId) {
    workflow().routeAfterFraud(orderId);
  }

  public static void startFulfillment(String orderId) {
    workflow().startFulfillment(orderId);
  }

  public static void reserveInventory(String orderId) {
    workflow().reserveInventory(orderId);
  }

  public static void chargePayment(String orderId) {
    workflow().chargePayment(orderId);
  }

  public static void createShipment(String orderId) {
    workflow().createShipment(orderId);
  }

  public static void sendReceipt(String orderId) {
    workflow().sendReceipt(orderId);
  }

  public static void applyReviewDecision(String orderId) {
    workflow().applyReviewDecision(orderId);
  }

  public static void markOrderFailed(String orderId) {
    workflow().markOrderFailed(orderId);
  }

  public static void reconcileOrders() {
    workflow().reconcileOrders();
  }

  private static OrderStreamService stream() {
    return CDI.current().select(OrderStreamService.class).get();
  }

  private static OrderScenarioService scenarios() {
    return CDI.current().select(OrderScenarioService.class).get();
  }

  private static OrderWorkflowService workflow() {
    return CDI.current().select(OrderWorkflowService.class).get();
  }
}
