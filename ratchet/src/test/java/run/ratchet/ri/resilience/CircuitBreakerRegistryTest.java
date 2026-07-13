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
package run.ratchet.ri.resilience;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.NoOpMetricsCollector;

class CircuitBreakerRegistryTest {

  @Test
  void serviceNamesContainingProfileSeparatorsDoNotUseEncodedStringKeys() throws Exception {
    CircuitBreakerRegistry registry = defaultRegistry();

    CircuitBreaker breaker = registry.getBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST);

    assertSame(breaker, registry.getBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST));
    assertFalse(registryKeyMap(registry).containsKey("payments:DEFAULT:FAST"));
  }

  @Test
  void serviceNamesContainingSeparatorsRemainIndependentlyManageable() {
    CircuitBreakerRegistry registry = defaultRegistry();

    CircuitBreaker plain = registry.getBreaker("payments", CircuitBreakerProfile.DEFAULT);
    CircuitBreaker colon = registry.getBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST);

    registry.openBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST);

    assertEquals(CircuitBreaker.State.CLOSED, plain.getState());
    assertEquals(CircuitBreaker.State.OPEN, colon.getState());
    assertEquals(
        CircuitBreaker.State.OPEN,
        registry.getBreakerState("payments:DEFAULT", CircuitBreakerProfile.FAST));
  }

  @Test
  void getBreakerState_createsMissingBreaker() {
    CircuitBreakerRegistry registry = defaultRegistry();

    assertEquals(CircuitBreaker.State.CLOSED, registry.getBreakerState("shipping"));
  }

  @Test
  void openBreaker_createsMissingBreakerBeforeOpening() {
    CircuitBreakerRegistry registry = defaultRegistry();

    registry.openBreaker("maintenance-service");

    assertEquals(CircuitBreaker.State.OPEN, registry.getBreakerState("maintenance-service"));
  }

  @Test
  void reportsServiceProfileAndInitialStateToMetricsCollector() {
    RecordingMetricsCollector metrics = new RecordingMetricsCollector();
    CircuitBreakerRegistry registry =
        new CircuitBreakerRegistry(
            new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults()), metrics);

    registry.getBreaker("payments", CircuitBreakerProfile.EXTERNAL_API);
    registry.openBreaker("payments", CircuitBreakerProfile.EXTERNAL_API);
    registry.resetBreaker("payments", CircuitBreakerProfile.EXTERNAL_API);

    assertEquals(
        List.of(
            "payments:EXTERNAL_API:CLOSED",
            "payments:EXTERNAL_API:OPEN",
            "payments:EXTERNAL_API:CLOSED"),
        metrics.transitions);
  }

  @SuppressWarnings("unchecked")
  private static Map<Object, CircuitBreaker> registryKeyMap(CircuitBreakerRegistry registry)
      throws NoSuchFieldException, IllegalAccessException {
    Field breakers = CircuitBreakerRegistry.class.getDeclaredField("breakers");
    breakers.setAccessible(true);
    return (Map<Object, CircuitBreaker>) breakers.get(registry);
  }

  private static CircuitBreakerRegistry defaultRegistry() {
    return new CircuitBreakerRegistry(
        new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults()));
  }

  private static final class RecordingMetricsCollector extends NoOpMetricsCollector {
    private final List<String> transitions = new ArrayList<>();

    @Override
    public void circuitBreakerState(String serviceName, String profile, String state) {
      transitions.add(serviceName + ":" + profile + ":" + state);
    }
  }
}
