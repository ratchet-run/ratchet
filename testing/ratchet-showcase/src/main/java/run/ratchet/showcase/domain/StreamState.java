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

import java.time.Duration;
import java.time.Instant;

public class StreamState {

  public boolean running;
  public long seed;
  public int ordersPerMinute;
  public double burstiness;
  public double failureMix;
  public long produced;
  public Instant startedAt;
  public Instant updatedAt;
  public Instant lastTickAt;
  public double tokenRemainder;

  public static StreamState stopped(long seed) {
    StreamState state = new StreamState();
    state.running = false;
    state.seed = seed;
    state.ordersPerMinute = 30;
    state.burstiness = 0.25;
    state.failureMix = 0.35;
    state.updatedAt = Instant.now();
    state.lastTickAt = state.updatedAt;
    return state;
  }

  public synchronized StreamState copy() {
    StreamState copy = new StreamState();
    copy.running = running;
    copy.seed = seed;
    copy.ordersPerMinute = ordersPerMinute;
    copy.burstiness = burstiness;
    copy.failureMix = failureMix;
    copy.produced = produced;
    copy.startedAt = startedAt;
    copy.updatedAt = updatedAt;
    copy.lastTickAt = lastTickAt;
    copy.tokenRemainder = tokenRemainder;
    return copy;
  }

  public synchronized void start(
      long newSeed, int newOrdersPerMinute, double newBurstiness, double newFailureMix) {
    Instant now = Instant.now();
    running = true;
    seed = newSeed;
    ordersPerMinute = clampRate(newOrdersPerMinute);
    burstiness = clamp(newBurstiness, 0.0, 1.0);
    failureMix = clamp(newFailureMix, 0.0, 1.0);
    startedAt = now;
    updatedAt = now;
    lastTickAt = now.minusSeconds(1);
    tokenRemainder = 1.0;
    produced = 0;
  }

  public synchronized void update(
      Integer newOrdersPerMinute, Long newSeed, Double newBurstiness, Double newFailureMix) {
    if (newOrdersPerMinute != null) {
      ordersPerMinute = clampRate(newOrdersPerMinute);
    }
    if (newSeed != null) {
      seed = newSeed;
    }
    if (newBurstiness != null) {
      burstiness = clamp(newBurstiness, 0.0, 1.0);
    }
    if (newFailureMix != null) {
      failureMix = clamp(newFailureMix, 0.0, 1.0);
    }
    updatedAt = Instant.now();
  }

  public synchronized void stop() {
    running = false;
    updatedAt = Instant.now();
  }

  public synchronized int drainDueOrders(Instant now) {
    if (!running) {
      lastTickAt = now;
      return 0;
    }
    Duration elapsed = Duration.between(lastTickAt, now);
    lastTickAt = now;
    double elapsedSeconds = Math.max(0.0, elapsed.toMillis() / 1000.0);
    double ordersPerSecond = ordersPerMinute / 60.0;
    double maxTokens = Math.max(1.0, ordersPerSecond * (1.0 + burstiness * 12.0));
    tokenRemainder = Math.min(maxTokens, tokenRemainder + elapsedSeconds * ordersPerSecond);
    int due = (int) Math.floor(tokenRemainder);
    tokenRemainder -= due;
    int capped = Math.min(due, 100);
    produced += capped;
    return capped;
  }

  private static int clampRate(int value) {
    return Math.max(1, Math.min(1000, value));
  }

  private static double clamp(double value, double min, double max) {
    return Math.max(min, Math.min(max, value));
  }
}
