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
package run.ratchet.ri.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerManager;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ResilienceStrategy;

class RatchetRuntimeDefaultsTest {

  @Test
  void classPolicyUsesSeparateConfiguredAllowlistsAndDenylist() {
    RatchetOptions options =
        RatchetOptions.builder()
            .security(
                security ->
                    security
                        .classPolicyAllowedPackages(Set.of("com.acme.jobs"))
                        .classPolicyAllowedResultTypePackages(Set.of("com.acme.results")))
            .build();

    ClassPolicy policy = RatchetRuntimeDefaults.classPolicy(options);

    assertTrue(policy.isAllowed("com.acme.jobs.DailyJob"));
    assertFalse(policy.isAllowed("com.acme.results.Result"));
    assertTrue(policy.isAllowedForResultType("com.acme.results.Result"));
    assertFalse(policy.isAllowedForResultType("com.acme.jobs.DailyJob"));
    assertFalse(policy.isAllowed("java.lang.Runtime"));
  }

  @Test
  void classPolicyFailsEmptyInvocationAllowlistWithContainerNeutralProperties() {
    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> RatchetRuntimeDefaults.classPolicy(RatchetOptions.defaults()));

    assertTrue(failure.getMessage().contains("ratchet.class-policy.allowed-packages"));
    assertTrue(failure.getMessage().contains("ratchet.allow-empty-class-policy"));
    assertFalse(failure.getMessage().contains("@Alternative"));
  }

  @Test
  void classPolicyAllowsExplicitEmptyOptIn() {
    RatchetOptions options =
        RatchetOptions.builder().security(security -> security.allowEmptyClassPolicy(true)).build();

    ClassPolicy policy = RatchetRuntimeDefaults.classPolicy(options);

    assertFalse(policy.isAllowed("com.acme.jobs.DailyJob"));
  }

  @Test
  void resilienceStrategyUsesTheSuppliedSharedRegistry() {
    RatchetOptions options =
        RatchetOptions.builder()
            .circuitBreaker(
                circuitBreaker ->
                    circuitBreaker.profile(
                        CircuitBreakerProfile.DEFAULT,
                        profile ->
                            profile
                                .minimumCalls(1)
                                .slidingWindowSize(1)
                                .failureRateThreshold(1.0f)))
            .build();
    CircuitBreakerManager manager = RatchetRuntimeDefaults.circuitBreakerRegistry(options);
    ResilienceStrategy strategy = RatchetRuntimeDefaults.resilienceStrategy(manager, options);

    assertThrows(IllegalStateException.class, () -> strategy.execute("shared", failingTask()));

    assertEquals("OPEN", manager.getBreaker("shared").stateName());
  }

  @Test
  void circuitBreakerRegistryUsesTheSuppliedConfigProvider() {
    AtomicInteger requests = new AtomicInteger();
    CircuitBreakerConfigProvider configProvider =
        new CircuitBreakerConfigProvider() {
          @Override
          public boolean isEnabled() {
            return true;
          }

          @Override
          public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
            requests.incrementAndGet();
            return new CircuitBreakerConfig(50.0f, 10, 1000, 2, 5);
          }
        };

    CircuitBreakerManager manager =
        RatchetRuntimeDefaults.circuitBreakerRegistry(
            configProvider, RatchetRuntimeDefaults.metricsCollector());

    assertNotNull(manager.getBreaker("custom"));
    assertTrue(requests.get() >= CircuitBreakerProfile.values().length);
  }

  @Test
  void exposesTheRemainingDefaultsThroughExportedTypes() {
    RatchetOptions options = RatchetOptions.defaults();

    assertNotNull(RatchetRuntimeDefaults.executorProvider());
    assertNotNull(RatchetRuntimeDefaults.metricsCollector());
    assertNotNull(RatchetRuntimeDefaults.tracingCollector());
    assertNotNull(RatchetRuntimeDefaults.clusterCoordinator());
    assertNotNull(RatchetRuntimeDefaults.errorSanitizer(options));
    assertNotNull(RatchetRuntimeDefaults.nodeTagAffinityProvider(options));
    assertEquals(ZoneOffset.UTC, RatchetRuntimeDefaults.clock().getZone());
    assertNotNull(RatchetRuntimeDefaults.jobAuthorizationPolicy());
    assertNotNull(RatchetRuntimeDefaults.jobInvocationResolver());
    assertNotNull(RatchetRuntimeDefaults.pollingStrategyProvider());
    assertNotNull(RatchetRuntimeDefaults.executionTuningProvider(options));
    assertNotNull(RatchetRuntimeDefaults.retryPolicy());
  }

  private static java.util.concurrent.Callable<Object> failingTask() {
    return () -> {
      throw new IllegalStateException("failed");
    };
  }
}
