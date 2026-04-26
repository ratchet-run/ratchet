package run.ratchet.tck.api;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concrete {@link TestClock} for runtimes that can be driven from a controllable clock. Implements
 * both {@link TestClock} (for the contract base classes) and {@link Clock} so an implementation can
 * inject this directly anywhere {@link Clock} is injected — including the RI's {@code
 * DefaultJobCreationService} via CDI alternative.
 *
 * <p>Seeded from {@link Clock#systemUTC()} at construction; thereafter only moves forward via
 * {@link #advance(Duration)} or {@link #advanceTo(Instant)}.
 */
public final class SteppingTestClock extends Clock implements TestClock {

  private final AtomicReference<Instant> current;

  public SteppingTestClock() {
    this(Clock.systemUTC().instant());
  }

  public SteppingTestClock(Instant start) {
    this.current = new AtomicReference<>(Objects.requireNonNull(start, "start"));
  }

  @Override
  public Instant now() {
    return current.get();
  }

  @Override
  public Instant instant() {
    return current.get();
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return this;
  }

  @Override
  public void advance(Duration delta) {
    Objects.requireNonNull(delta, "delta");
    if (delta.isNegative()) {
      throw new IllegalArgumentException("advance(delta) requires non-negative delta: " + delta);
    }
    current.updateAndGet(prev -> prev.plus(delta));
  }

  @Override
  public void advanceTo(Instant target) {
    Objects.requireNonNull(target, "target");
    current.updateAndGet(
        prev -> {
          if (target.isBefore(prev)) {
            throw new IllegalArgumentException(
                "advanceTo(target) requires target >= now(): target=" + target + ", now=" + prev);
          }
          return target;
        });
  }
}
