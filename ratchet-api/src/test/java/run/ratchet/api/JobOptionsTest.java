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
package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class JobOptionsTest {

  @Test
  void defaults_returnsExpectedValues() {
    JobOptions opts = JobOptions.defaults();

    assertEquals(JobPriority.NORMAL, opts.priority());
    assertEquals(0, opts.maxRetries());
    assertEquals(BackoffPolicy.NONE, opts.backoffPolicy());
    assertEquals(Duration.ZERO, opts.backoffParam());
    assertEquals(0, opts.timeoutSec());
  }

  @Test
  void withMaxRetries_returnsNewInstanceWithUpdatedRetries() {
    JobOptions original = JobOptions.defaults();
    JobOptions updated = original.withMaxRetries(5);

    assertEquals(5, updated.maxRetries());
    // original is unchanged (immutability)
    assertEquals(0, original.maxRetries());
    // other fields preserved
    assertEquals(original.priority(), updated.priority());
    assertEquals(original.backoffPolicy(), updated.backoffPolicy());
    assertEquals(original.backoffParam(), updated.backoffParam());
    assertEquals(original.timeoutSec(), updated.timeoutSec());
  }

  @Test
  void withMaxRetries_rejectsNegativeRetries() {
    assertThrows(IllegalArgumentException.class, () -> JobOptions.defaults().withMaxRetries(-1));
  }

  @Test
  void withPriority_returnsNewInstancePreservingOtherFields() {
    JobOptions original = JobOptions.defaults().withMaxRetries(3);
    JobOptions updated = original.withPriority(JobPriority.HIGH);

    assertEquals(JobPriority.HIGH, updated.priority());
    assertEquals(JobPriority.NORMAL, original.priority());
    assertEquals(3, updated.maxRetries());
  }

  @Test
  void withBackoff_setsPolicyAndParam() {
    Duration param = Duration.ofSeconds(5);
    JobOptions opts = JobOptions.defaults().withBackoff(BackoffPolicy.EXPONENTIAL, param);

    assertEquals(BackoffPolicy.EXPONENTIAL, opts.backoffPolicy());
    assertEquals(param, opts.backoffParam());
  }

  @Test
  void withTimeout_convertsDurationToSeconds() {
    JobOptions opts = JobOptions.defaults().withTimeout(Duration.ofMinutes(10));

    assertEquals(600, opts.timeoutSec());
  }

  @Test
  void withTimeout_zeroDisablesTimeout() {
    JobOptions opts = JobOptions.defaults().withTimeout(Duration.ZERO);

    assertEquals(0, opts.timeoutSec());
  }

  @Test
  void withTimeout_rejectsSubSecondPositiveDuration() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> JobOptions.defaults().withTimeout(Duration.ofMillis(500)));

    assertTrue(ex.getMessage().contains("at least 1 second"));
  }

  @Test
  void withTimeout_acceptsOneSecond() {
    JobOptions opts = JobOptions.defaults().withTimeout(Duration.ofSeconds(1));

    assertEquals(1, opts.timeoutSec());
  }

  @Test
  void withTimeout_rejectsNegativeDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> JobOptions.defaults().withTimeout(Duration.ofSeconds(-1)));
  }

  @Test
  void withTimeout_rejectsDurationsOutsideIntegerRangeWithApiException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobOptions.defaults()
                    .withTimeout(Duration.ofSeconds((long) Integer.MAX_VALUE + 1L)));

    assertTrue(ex.getMessage().contains("timeout must be <= " + Integer.MAX_VALUE + " seconds"));
  }

  @Test
  void constructorRejectsInvalidValues() {
    assertThrows(
        NullPointerException.class,
        () -> new JobOptions(null, 0, BackoffPolicy.NONE, Duration.ZERO, 0));
    assertThrows(
        NullPointerException.class,
        () -> new JobOptions(JobPriority.NORMAL, 0, null, Duration.ZERO, 0));
    assertThrows(
        NullPointerException.class,
        () -> new JobOptions(JobPriority.NORMAL, 0, BackoffPolicy.NONE, null, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new JobOptions(JobPriority.NORMAL, 0, BackoffPolicy.NONE, Duration.ZERO, -1));
  }

  @Test
  void builderChain_preservesAllFields() {
    Duration backoffDuration = Duration.ofSeconds(2);
    JobOptions opts =
        JobOptions.defaults()
            .withPriority(JobPriority.CRITICAL)
            .withMaxRetries(10)
            .withBackoff(BackoffPolicy.FIXED, backoffDuration)
            .withTimeout(Duration.ofMinutes(5));

    assertEquals(JobPriority.CRITICAL, opts.priority());
    assertEquals(10, opts.maxRetries());
    assertEquals(BackoffPolicy.FIXED, opts.backoffPolicy());
    assertEquals(backoffDuration, opts.backoffParam());
    assertEquals(300, opts.timeoutSec());
  }

  @Test
  void recordEquality_sameValuesAreEqual() {
    JobOptions a = JobOptions.defaults().withMaxRetries(3);
    JobOptions b = JobOptions.defaults().withMaxRetries(3);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
