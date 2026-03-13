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
