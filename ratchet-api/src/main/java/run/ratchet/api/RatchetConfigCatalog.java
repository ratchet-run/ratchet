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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.internal.RatchetConfigKeys;
import run.ratchet.spi.RatchetConfigKey;

/** Canonical catalog of configuration properties understood by the Ratchet runtime. */
@Incubating
public final class RatchetConfigCatalog {

  private static final List<Entry> ENTRIES = buildEntries();

  private RatchetConfigCatalog() {}

  /** Returns every canonical configuration entry in deterministic order. */
  public static List<Entry> entries() {
    return ENTRIES;
  }

  private static List<Entry> buildEntries() {
    List<Entry> entries = new ArrayList<>();
    RatchetConfigKeys.fixedKeys().stream().map(Entry::from).forEach(entries::add);

    for (RatchetConfigVocabulary.ExecutionType executionType :
        RatchetConfigVocabulary.EXECUTION_TYPES) {
      entries.add(Entry.from(RatchetConfigKeys.virtualThreadLimit(executionType.name())));
      entries.add(Entry.from(RatchetConfigKeys.rateLimitPerMinute(executionType.name())));
    }

    RatchetOptions defaults = RatchetOptions.defaults();
    for (CircuitBreakerProfile profile : RatchetConfigVocabulary.CIRCUIT_BREAKER_PROFILES) {
      RatchetOptions.CircuitBreakerProfileOptions profileDefaults =
          defaults.circuitBreaker().profile(profile);
      String profileName = profile.name();
      entries.add(
          Entry.from(
              RatchetConfigKeys.circuitBreakerFailureRate(
                  profileName, profileDefaults.failureRateThreshold())));
      entries.add(
          Entry.from(
              RatchetConfigKeys.circuitBreakerWindowSize(
                  profileName, profileDefaults.slidingWindowSize())));
      entries.add(
          Entry.from(
              RatchetConfigKeys.circuitBreakerWaitMs(
                  profileName, profileDefaults.waitDurationMs())));
      entries.add(
          Entry.from(
              RatchetConfigKeys.circuitBreakerHalfOpenCalls(
                  profileName, profileDefaults.permittedCallsInHalfOpen())));
      entries.add(
          Entry.from(
              RatchetConfigKeys.circuitBreakerMinimumCalls(
                  profileName, profileDefaults.minimumCalls())));
    }
    return List.copyOf(entries);
  }

  /** A canonical property name and its environment-variable equivalent. */
  public record Entry(String propertyName, String environmentVariable) {

    public Entry {
      Objects.requireNonNull(propertyName, "propertyName must not be null");
      Objects.requireNonNull(environmentVariable, "environmentVariable must not be null");
    }

    private static Entry from(RatchetConfigKey<?> key) {
      return new Entry(key.name(), key.environmentVariable());
    }
  }
}
