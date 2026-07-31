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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import run.ratchet.api.internal.RatchetConfigKeys;
import run.ratchet.spi.RatchetConfigKey;

class RatchetConfigCatalogTest {

  @Test
  void containsEveryFixedKeyAndEveryExpandedFamilyMember() throws IllegalAccessException {
    List<RatchetConfigCatalog.Entry> entries = RatchetConfigCatalog.entries();
    Set<RatchetConfigCatalog.Entry> entrySet = Set.copyOf(entries);

    Set<RatchetConfigCatalog.Entry> reflectedFixedKeys =
        Arrays.stream(RatchetConfigKeys.class.getFields())
            .filter(field -> Modifier.isStatic(field.getModifiers()))
            .filter(field -> RatchetConfigKey.class.isAssignableFrom(field.getType()))
            .map(RatchetConfigCatalogTest::readKey)
            .map(RatchetConfigCatalogTest::entry)
            .collect(Collectors.toUnmodifiableSet());

    assertTrue(entrySet.containsAll(reflectedFixedKeys));

    for (RatchetConfigVocabulary.ExecutionType executionType :
        RatchetConfigVocabulary.EXECUTION_TYPES) {
      assertTrue(
          entrySet.contains(entry(RatchetConfigKeys.virtualThreadLimit(executionType.name()))));
      assertTrue(
          entrySet.contains(entry(RatchetConfigKeys.rateLimitPerMinute(executionType.name()))));
    }

    RatchetOptions defaults = RatchetOptions.defaults();
    for (CircuitBreakerProfile profile : CircuitBreakerProfile.values()) {
      RatchetOptions.CircuitBreakerProfileOptions profileDefaults =
          defaults.circuitBreaker().profile(profile);
      String profileName = profile.name();
      assertTrue(
          entrySet.contains(
              entry(
                  RatchetConfigKeys.circuitBreakerFailureRate(
                      profileName, profileDefaults.failureRateThreshold()))));
      assertTrue(
          entrySet.contains(
              entry(
                  RatchetConfigKeys.circuitBreakerWindowSize(
                      profileName, profileDefaults.slidingWindowSize()))));
      assertTrue(
          entrySet.contains(
              entry(
                  RatchetConfigKeys.circuitBreakerWaitMs(
                      profileName, profileDefaults.waitDurationMs()))));
      assertTrue(
          entrySet.contains(
              entry(
                  RatchetConfigKeys.circuitBreakerHalfOpenCalls(
                      profileName, profileDefaults.permittedCallsInHalfOpen()))));
      assertTrue(
          entrySet.contains(
              entry(
                  RatchetConfigKeys.circuitBreakerMinimumCalls(
                      profileName, profileDefaults.minimumCalls()))));
    }

    int expectedSize =
        reflectedFixedKeys.size()
            + (RatchetConfigVocabulary.EXECUTION_TYPES.size() * 2)
            + (CircuitBreakerProfile.values().length * 5);
    assertEquals(expectedSize, entries.size());
    assertEquals(entries.size(), entrySet.size());
    assertFalse(
        entries.stream()
            .anyMatch(entry -> "ratchet.worker.use-virtual-threads".equals(entry.propertyName())));
  }

  private static RatchetConfigKey<?> readKey(Field field) {
    try {
      return (RatchetConfigKey<?>) field.get(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
  }

  private static RatchetConfigCatalog.Entry entry(RatchetConfigKey<?> key) {
    return new RatchetConfigCatalog.Entry(key.name(), key.environmentVariable());
  }
}
