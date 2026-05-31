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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;

class BackoffPolicyHandlerTest {

  private static final long MAX_EXPONENTIAL_DELAY_MS = 86_400_000L;

  @Test
  void none_returnsZeroRegardlessOfBaseOrAttempts() {
    assertEquals(0L, BackoffPolicyHandler.computeDelay(BackoffPolicy.NONE, 5000, 1));
    assertEquals(0L, BackoffPolicyHandler.computeDelay(BackoffPolicy.NONE, 0, 100));
  }

  @Test
  void fixed_returnsBaseMsForFirstAttempt() {
    assertEquals(3000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 3000, 1));
  }

  @Test
  void fixed_returnsBaseMsForLaterAttempts() {
    assertEquals(3000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 3000, 10));
  }

  @Test
  void fixed_ignoresAttemptBoundaries() {
    assertEquals(3000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 3000, 0));
    assertEquals(3000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 3000, 2));
    assertEquals(
        3000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 3000, Integer.MAX_VALUE));
  }

  @Test
  void fixed_zeroBaseMs_returnsZero() {
    assertEquals(0L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 0, 5));
  }

  @Test
  void exponential_attempt1_returnsBaseMs() {
    assertEquals(1000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, 1));
  }

  @Test
  void exponential_attempt2_doublesBaseMs() {
    assertEquals(2000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, 2));
  }

  @Test
  void exponential_attempt5_returns16xBaseMs() {
    assertEquals(16_000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, 5));
  }

  @Test
  void exponential_cappedAt24Hours() {
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 10_000, 30);
    assertEquals(MAX_EXPONENTIAL_DELAY_MS, delay);
  }

  @Test
  void exponential_maxExponentCappedAt20() {
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1, 22);
    long delayAtMaxExponent = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1, 21);
    assertEquals(
        delayAtMaxExponent, delay, "Exponent should be capped at 20 regardless of attempt");
  }

  @Test
  void exponential_veryLargeAttempt_doesNotOverflow() {
    long delay =
        BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, Integer.MAX_VALUE);
    assertTrue(delay > 0, "Delay must be positive even for huge attempt numbers");
    assertTrue(delay <= MAX_EXPONENTIAL_DELAY_MS, "Delay must not exceed the 24-hour cap");
  }

  @Test
  void exponential_overflowGuard_largeBaseMsAndHighAttempt() {
    long delay =
        BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, Integer.MAX_VALUE, 21);
    assertEquals(MAX_EXPONENTIAL_DELAY_MS, delay);
  }

  @Test
  void exponential_zeroBaseMs_returnsZero() {
    assertEquals(0L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 0, 5));
  }

  @Test
  void exponential_negativeAttempt_handledGracefully() {
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, -1);
    assertTrue(delay <= MAX_EXPONENTIAL_DELAY_MS, "Delay must not exceed the 24-hour cap");
  }

  @Test
  void exponential_attempt1_withLargeBase_capsAtMax() {
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 100_000_000, 1);
    assertEquals(MAX_EXPONENTIAL_DELAY_MS, delay);
  }
}
