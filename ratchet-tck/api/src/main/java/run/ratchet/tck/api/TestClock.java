package run.ratchet.tck.api;

import java.time.Duration;
import java.time.Instant;

/**
 * Deterministic time source for TCK contracts that exercise scheduling delays, retry backoffs, or
 * cron-driven recurring jobs without depending on wall-clock progression.
 *
 * <p>Implementations expose this only when their scheduler can be driven from a controllable clock.
 * Implementations whose executor is hardwired to wall-clock time MUST return {@link
 * java.util.Optional#empty()} from {@link RatchetTckRuntime#clock()}; contracts that require a
 * clock will then skip via {@code Assumptions.assumeTrue(...)} rather than fail.
 */
public interface TestClock {

  /** Current logical instant. Never null. */
  Instant now();

  /** Advances the logical clock by the given non-negative duration. */
  void advance(Duration delta);

  /** Sets the logical clock to the given instant. Must be at or after {@link #now()}. */
  void advanceTo(Instant target);
}
