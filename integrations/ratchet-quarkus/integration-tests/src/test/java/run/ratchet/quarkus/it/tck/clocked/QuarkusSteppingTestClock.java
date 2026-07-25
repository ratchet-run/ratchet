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
package run.ratchet.quarkus.it.tck.clocked;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import run.ratchet.tck.api.SteppingTestClock;
import run.ratchet.tck.api.TestClock;

/**
 * Quarkus-compatible class-level clock alternative for the clocked TCK profile.
 *
 * <p>ArC only accepts classes from {@code QuarkusTestProfile#getEnabledAlternatives()}, so this
 * bean exposes one shared {@link SteppingTestClock} under both {@link Clock} and {@link TestClock}
 * bean types without relying on producer-method alternatives.
 */
@Alternative
@Singleton
public class QuarkusSteppingTestClock extends Clock implements TestClock {

  private final SteppingTestClock delegate = new SteppingTestClock();

  @Override
  public Instant now() {
    return delegate.now();
  }

  @Override
  public long millis() {
    return delegate.millis();
  }

  @Override
  public Instant instant() {
    return delegate.instant();
  }

  @Override
  public ZoneId getZone() {
    return delegate.getZone();
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return delegate.withZone(zone);
  }

  @Override
  public void advance(Duration delta) {
    delegate.advance(delta);
  }

  @Override
  public void advanceTo(Instant target) {
    delegate.advanceTo(target);
  }
}
