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
package run.ratchet.showcase.domain;

import java.time.Instant;

public class OrderProjection {

  public String orderId;
  public long sequence;
  public Instant createdAt;
  public Instant updatedAt;
  public String customer;
  public String sku;
  public int quantity;
  public String warehouse;
  public boolean vip;
  public int fraudScore;
  public String paymentProfile;
  public String carrier;
  public boolean addressBad;
  public boolean inventoryPressure;
  public OrderStatus status;
  public String message;
  public String currentJobId;

  public static OrderProjection from(GeneratedOrder generated) {
    OrderProjection order = new OrderProjection();
    order.orderId = generated.orderId();
    order.sequence = generated.sequence();
    order.createdAt = generated.createdAt();
    order.updatedAt = generated.createdAt();
    order.customer = generated.customer();
    order.sku = generated.sku();
    order.quantity = generated.quantity();
    order.warehouse = generated.warehouse();
    order.vip = generated.vip();
    order.fraudScore = generated.fraudScore();
    order.paymentProfile = generated.paymentProfile();
    order.carrier = generated.carrier();
    order.addressBad = generated.addressBad();
    order.inventoryPressure = generated.inventoryPressure();
    order.status = OrderStatus.RECEIVED;
    order.message = "Order received";
    return order;
  }

  public synchronized void transition(OrderStatus newStatus, String newMessage) {
    status = newStatus;
    message = newMessage;
    updatedAt = Instant.now();
  }

  public synchronized void attachJob(String jobId) {
    currentJobId = jobId;
    updatedAt = Instant.now();
  }

  public synchronized OrderStatus status() {
    return status;
  }

  public synchronized OrderProjection snapshot() {
    OrderProjection copy = new OrderProjection();
    copy.orderId = orderId;
    copy.sequence = sequence;
    copy.createdAt = createdAt;
    copy.updatedAt = updatedAt;
    copy.customer = customer;
    copy.sku = sku;
    copy.quantity = quantity;
    copy.warehouse = warehouse;
    copy.vip = vip;
    copy.fraudScore = fraudScore;
    copy.paymentProfile = paymentProfile;
    copy.carrier = carrier;
    copy.addressBad = addressBad;
    copy.inventoryPressure = inventoryPressure;
    copy.status = status;
    copy.message = message;
    copy.currentJobId = currentJobId;
    return copy;
  }
}
