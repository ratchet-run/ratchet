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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import run.ratchet.showcase.domain.StreamState;

class StreamStateTest {

  @Test
  void drainsConfiguredOrdersPerMinuteThroughTokenBucket() {
    StreamState state = StreamState.stopped(1L);
    state.start(1L, 120, 0.0, 0.0);

    int due = state.drainDueOrders(Instant.now().plusSeconds(1));

    assertTrue(due >= 1 && due <= 3);
  }

  @Test
  void clampsDashboardRateToSupportedRange() {
    StreamState state = StreamState.stopped(1L);
    state.start(1L, 10_000, 0.0, 0.0);

    assertEquals(1000, state.copy().ordersPerMinute);
  }

  @Test
  void updatingSeedOrRatePreservesProducedCount() {
    StreamState state = StreamState.stopped(1L);
    state.start(1L, 120, 0.0, 0.0);
    state.drainDueOrders(Instant.now().plusSeconds(2));
    long produced = state.copy().produced;

    state.update(60, 2L, 0.25, 0.35);

    assertEquals(produced, state.copy().produced);
    assertEquals(60, state.copy().ordersPerMinute);
    assertEquals(2L, state.copy().seed);
  }
}
