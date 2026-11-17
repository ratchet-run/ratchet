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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.internal.ReleaseRetries.Policy;

class ReleaseRetriesTest {

  @Test
  void policy_rejectsADelayThatIsNotPositive() {
    // A zero first delay would turn a failed handoff into a busy loop on the scheduler.
    assertThrows(
        IllegalArgumentException.class,
        () -> Policy.untilReleased(Duration.ZERO, Duration.ofMinutes(1)));
    assertThrows(IllegalArgumentException.class, () -> Policy.bounded(Duration.ofSeconds(-1), 3));
  }

  @Test
  void policy_rejectsAMaxDelayBelowTheFirstDelay() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Policy.untilReleased(Duration.ofMinutes(1), Duration.ofSeconds(5)));
  }

  @Test
  void policy_rejectsARetryLimitBelowOne() {
    assertThrows(IllegalArgumentException.class, () -> Policy.bounded(Duration.ofSeconds(5), 0));
  }
}
