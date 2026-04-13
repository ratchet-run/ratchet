package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import org.junit.jupiter.api.Test;

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
  void fixed_zeroBaseMs_returnsZero() {
    assertEquals(0L, BackoffPolicyHandler.computeDelay(BackoffPolicy.FIXED, 0, 5));
  }

  @Test
  void exponential_attempt1_returnsBaseMs() {
    // 2^(1-1) = 1 → baseMs * 1
    assertEquals(1000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, 1));
  }

  @Test
  void exponential_attempt2_doublesBaseMs() {
    // 2^(2-1) = 2 → 1000 * 2
    assertEquals(2000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, 2));
  }

  @Test
  void exponential_attempt5_returns16xBaseMs() {
    // 2^(5-1) = 16 → 1000 * 16
    assertEquals(16_000L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, 5));
  }

  @Test
  void exponential_cappedAt24Hours() {
    // Even with high attempt count, should not exceed 24h
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 10_000, 30);
    assertEquals(MAX_EXPONENTIAL_DELAY_MS, delay);
  }

  @Test
  void exponential_maxExponentCappedAt20() {
    // attempt 22 → exponent capped at 20 → 2^20 = 1_048_576
    // 1 * 1_048_576 = 1_048_576 (under 24h cap)
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
    // baseMs * 2^20 would overflow for large baseMs → should return cap
    long delay =
        BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, Integer.MAX_VALUE, 21);
    assertEquals(MAX_EXPONENTIAL_DELAY_MS, delay);
  }

  @Test
  void exponential_zeroBaseMs_returnsZero() {
    // 0 * anything = 0, which is under the cap
    assertEquals(0L, BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 0, 5));
  }

  @Test
  void exponential_negativeAttempt_handledGracefully() {
    // attempts - 1 = negative → Math.min(-2, 20) = -2 → 1L << -2 is implementation-defined
    // but should not throw and should be capped
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 1000, -1);
    assertTrue(delay <= MAX_EXPONENTIAL_DELAY_MS, "Delay must not exceed the 24-hour cap");
  }

  @Test
  void exponential_attempt1_withLargeBase_capsAtMax() {
    // 2^0 = 1, so delay = baseMs; but if baseMs > 24h cap, cap applies
    long delay = BackoffPolicyHandler.computeDelay(BackoffPolicy.EXPONENTIAL, 100_000_000, 1);
    assertEquals(MAX_EXPONENTIAL_DELAY_MS, delay);
  }
}
