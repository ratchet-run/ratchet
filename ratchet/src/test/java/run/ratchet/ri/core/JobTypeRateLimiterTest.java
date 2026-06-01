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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobExecutionType;

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
  void getCurrentCount_excludesRejectedAttempts() {
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 2)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options);

    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE));
    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE));
    assertFalse(limiter.tryAcquire(JobExecutionType.SINGLE));

    assertEquals(
        2,
        limiter.getCurrentCount(JobExecutionType.SINGLE),
        "rejected calls must not consume rate-limit capacity");
  }

  @Test
  void tryAcquire_limitOneRejectsSecondCallUntilWindowResets() {
    AtomicLong now = new AtomicLong(1_000L);
    RatchetOptions options =
        RatchetOptions.builder().execution(e -> e.rateLimitPerMinute("SINGLE", 1)).build();
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(options, now::get);

    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE));
    assertFalse(limiter.tryAcquire(JobExecutionType.SINGLE));
    assertEquals(1, limiter.getCurrentCount(JobExecutionType.SINGLE));

    now.addAndGet(60_000L);

    assertEquals(0, limiter.getCurrentCount(JobExecutionType.SINGLE));
    assertTrue(limiter.tryAcquire(JobExecutionType.SINGLE));
    assertEquals(1, limiter.getCurrentCount(JobExecutionType.SINGLE));
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
  void nullJobTypeIsTreatedAsUnlimited() {
    JobTypeRateLimiter limiter = new JobTypeRateLimiter(RatchetOptions.defaults());

    assertEquals(0, limiter.getRateLimit(null));
    assertFalse(limiter.isRateLimited(null));
    assertTrue(limiter.tryAcquire(null));
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
