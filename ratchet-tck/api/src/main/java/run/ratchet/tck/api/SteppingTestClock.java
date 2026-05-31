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
 *
 * @apiNote <b>Internal.</b> This clock is published from the TCK so RI tests (notably {@code
 *     DefaultJobCreationService}'s alternative wiring) can inject it directly. It is a test-only
 *     fixture and is NOT a general-purpose {@link Clock}: {@link #withZone(ZoneId)} deliberately
 *     returns {@code this} rather than a zone-adjusted view (see the {@link #withZone(ZoneId)}
 *     {@code @implNote}), which violates the {@link Clock} contract. Production code MUST NOT
 *     depend on this class.
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

  /**
   * {@inheritDoc}
   *
   * @implNote This clock is always UTC; the zone argument is ignored and {@code this} is returned.
   *     Zone-sensitive formatting should call {@code clock.instant().atZone(zone)} instead. This
   *     intentional deviation from the {@link Clock} contract keeps the fixture's {@link
   *     AtomicReference} tick state shared across callers.
   */
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
