package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobExecutionType;
import org.junit.jupiter.api.Test;

class JobTypeRateLimiterTest {

  @Test
  void tryAcquire_unlimited_alwaysReturnsTrue() {
    // Default options configure all limits to 0 (unlimited)
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(RatchetOptions.defaults());

    for (int i = 0; i < 1000; i++) {
      assertTrue(
          limiter.tryAcquire(JobExecutionType.SINGLE), "unlimited limiter must always allow");
    }
  }

  @Test
  void tryAcquire_withinLimit_returnsTrue() {
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 3)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE), "1st call within limit of 3");
    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE), "2nd call within limit of 3");
    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE), "3rd call at limit of 3");
  }

  @Test
  void tryAcquire_exceedsLimit_returnsFalse() {
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 3)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    limiter.tryAcquire(JobExecutionType.SINGLE);
    limiter.tryAcquire(JobExecutionType.SINGLE);
    limiter.tryAcquire(JobExecutionType.SINGLE);

    assertFalse(limiter.tryAcquire(JobExecutionType.SINGLE), "4th call must be rejected");
  }

  @Test
  void getCurrentCount_incrementsBeforeCheck_includesRejectedAttempts() {
    // The RateWindow increments count BEFORE the limit check, so rejected calls also inflate it.
    // This test documents the known semantic: getCurrentCount() counts attempts, not successes.
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 2)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    limiter.tryAcquire(JobExecutionType.SINGLE); // count → 1, allowed
    limiter.tryAcquire(JobExecutionType.SINGLE); // count → 2, allowed
    limiter.tryAcquire(JobExecutionType.SINGLE); // count → 3, rejected

    assertEquals(
        3,
        limiter.getCurrentCount(JobExecutionType.SINGLE),
        "getCurrentCount includes the rejected call because increment precedes the check");
  }

  @Test
  void isRateLimited_matchesConfiguration() {
    RatchetOptions withLimit =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 5)).build();
    JobTypeRateLimiter limited = new JobTypeRateLimiter(withLimit);

    JobTypeRateLimiter unlimited = new JobTypeRateLimiter(RatchetOptions.defaults());

    assertTrue(limited.isRateLimited(JobExecutionType.SINGLE));
    assertFalse(unlimited.isRateLimited(JobExecutionType.SINGLE));
  }

  @Test
  void getRateLimit_returnsConfiguredValue() {
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("BATCH_CHILD", 10)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    assertEquals(10, limiter.getRateLimit(JobExecutionType.BATCH_CHILD));
    assertEquals(
        0, limiter.getRateLimit(JobExecutionType.SINGLE), "unconfigured type defaults to 0");
  }

  @Test
  void getCurrentCount_noCallsMade_returnsZero() {
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 5)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    assertEquals(0, limiter.getCurrentCount(JobExecutionType.SINGLE));
  }

  @Test
  void tryAcquire_differentTypesAreIndependent() {
    RatchetOptions options =
        RatchetOptions.builder()
            .execution(e -> e.rateLimitPerMinute("SINGLE", 1).rateLimitPerMinute("BATCH_CHILD", 1))
            .build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE), "SINGLE first call");
    assertFalse(limiter.tryAcquire(JobExecutionType.SINGLE), "SINGLE second call rejected");
    assertTrue(
        limiter.tryAcquire(JobExecutionType.BATCH_CHILD),
        "BATCH_CHILD is a separate window, still within limit");
  }
}
