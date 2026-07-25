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
package run.ratchet.quarkus.it.tck;

import java.util.LinkedHashSet;
import java.util.Set;
import run.ratchet.quarkus.it.tck.clocked.InMemoryJobStore;
import run.ratchet.quarkus.it.tck.clocked.QuarkusClockedTckRuntime;
import run.ratchet.quarkus.it.tck.clocked.QuarkusSteppingTestClock;

/** Quarkus TCK profile that enables the clocked delayed-scheduling store and clock overrides. */
public class QuarkusClockedProfile extends QuarkusRatchetTckProfile {

  @Override
  public Set<Class<?>> getEnabledAlternatives() {
    Set<Class<?>> enabledAlternatives = new LinkedHashSet<>(super.getEnabledAlternatives());
    enabledAlternatives.add(InMemoryJobStore.class);
    enabledAlternatives.add(QuarkusClockedTckRuntime.class);
    enabledAlternatives.add(QuarkusSteppingTestClock.class);
    return Set.copyOf(enabledAlternatives);
  }
}
