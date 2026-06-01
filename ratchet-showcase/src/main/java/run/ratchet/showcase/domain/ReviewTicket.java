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

public class ReviewTicket {

  public String id;
  public String orderId;
  public int fraudScore;
  public String customer;
  public boolean vip;
  public String status;
  public String decision;
  public String reason;
  public Instant createdAt;
  public Instant updatedAt;

  public ReviewTicket() {}

  public ReviewTicket(String id, OrderProjection order) {
    this.id = id;
    this.orderId = order.orderId;
    this.fraudScore = order.fraudScore;
    this.customer = order.customer;
    this.vip = order.vip;
    this.status = "OPEN";
    this.createdAt = Instant.now();
    this.updatedAt = createdAt;
  }

  public synchronized void decide(String newDecision, String newReason) {
    decision = newDecision;
    reason = newReason;
    status = "CLOSED";
    updatedAt = Instant.now();
  }

  public synchronized boolean isOpen() {
    return "OPEN".equals(status);
  }

  public synchronized ReviewTicket snapshot() {
    ReviewTicket copy = new ReviewTicket();
    copy.id = id;
    copy.orderId = orderId;
    copy.fraudScore = fraudScore;
    copy.customer = customer;
    copy.vip = vip;
    copy.status = status;
    copy.decision = decision;
    copy.reason = reason;
    copy.createdAt = createdAt;
    copy.updatedAt = updatedAt;
    return copy;
  }
}
