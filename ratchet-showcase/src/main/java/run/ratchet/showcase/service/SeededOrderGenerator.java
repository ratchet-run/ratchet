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
import java.util.SplittableRandom;
import run.ratchet.showcase.domain.GeneratedOrder;

@ApplicationScoped
public class SeededOrderGenerator {

  public static final long DEFAULT_SEED = 8675309L;

  private static final String[] CUSTOMERS = {
    "Acme Field Services",
    "Northwind Retail",
    "Meridian Health",
    "Signal Labs",
    "Lakefront Supply",
    "Vertex Aerospace",
    "Cobalt Books",
    "Summit Outfitters"
  };
  private static final String[] SKUS = {
    "SKU-ROBOT-ARM",
    "SKU-SENSOR-KIT",
    "SKU-COLD-PACK",
    "SKU-DRONE-BAY",
    "SKU-LEDGER-PRO",
    "SKU-CABLE-CRATE",
    "SKU-VIAL-BOX",
    "SKU-SAFETY-RFID"
  };
  private static final String[] WAREHOUSES = {"PHX-1", "DFW-2", "ABE-3", "RNO-4"};
  private static final String[] CARRIERS = {"UPS", "FedEx", "DHL", "Regional Courier"};

  public GeneratedOrder generate(long seed, long sequence, double failureMix) {
    SplittableRandom random = new SplittableRandom(mix(seed, sequence));
    String orderId = "ORD-" + String.format("%06d", sequence);
    String customer = pick(random, CUSTOMERS);
    String sku = pick(random, SKUS);
    int quantity = 1 + random.nextInt(8);
    boolean vip = random.nextDouble() < 0.18;
    boolean inventoryPressure = random.nextDouble() < 0.20 + failureMix * 0.22;
    boolean addressBad = random.nextDouble() < failureMix * 0.035;
    String paymentProfile = paymentProfile(random, failureMix);
    int fraudScore =
        fraudScore(random, vip, paymentProfile, addressBad, inventoryPressure, failureMix);
    return new GeneratedOrder(
        orderId,
        sequence,
        Instant.EPOCH.plusSeconds(sequence),
        customer,
        sku,
        quantity,
        pick(random, WAREHOUSES),
        vip,
        fraudScore,
        paymentProfile,
        pick(random, CARRIERS),
        addressBad,
        inventoryPressure);
  }

  private static String paymentProfile(SplittableRandom random, double failureMix) {
    double roll = random.nextDouble();
    if (roll < failureMix * 0.045) {
      return "BAD_CARD";
    }
    if (roll < failureMix * 0.18) {
      return "TRANSIENT_GATEWAY";
    }
    return "NORMAL";
  }

  private static int fraudScore(
      SplittableRandom random,
      boolean vip,
      String paymentProfile,
      boolean addressBad,
      boolean inventoryPressure,
      double failureMix) {
    int score = 12 + random.nextInt(50);
    score += (int) Math.round(failureMix * 14);
    if (vip) {
      score -= 8;
    }
    if (!"NORMAL".equals(paymentProfile)) {
      score += 14;
    }
    if (addressBad) {
      score += 28;
    }
    if (inventoryPressure) {
      score += 6;
    }
    return Math.max(1, Math.min(99, score));
  }

  private static String pick(SplittableRandom random, String[] values) {
    return values[random.nextInt(values.length)];
  }

  private static long mix(long seed, long sequence) {
    long z = seed + 0x9E3779B97F4A7C15L * (sequence + 1);
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }
}
