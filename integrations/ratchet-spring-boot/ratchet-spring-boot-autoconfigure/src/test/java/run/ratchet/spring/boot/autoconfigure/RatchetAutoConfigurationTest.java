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
package run.ratchet.spring.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerManager;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.TracingCollector;

class RatchetAutoConfigurationTest {

  @Test
  void bindsOptionsAndSuppliesTheCompleteExportedDefaultInventory() {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context
          .getEnvironment()
          .getPropertySources()
          .addFirst(
              new MapPropertySource(
                  "test",
                  Map.of(
                      "ratchet.poller.batch-size", "31",
                      "ratchet.class-policy.allowed-packages", "run.ratchet")));
      context.register(RatchetAutoConfiguration.class);
      context.refresh();

      assertEquals(31, context.getBean(RatchetOptions.class).polling().batchSize());
      for (Class<?> defaultType : exportedDefaultTypes()) {
        assertNotNull(context.getBean(defaultType), () -> "missing default " + defaultType);
      }
    }
  }

  @Test
  void userRatchetOptionsBacksOffTheBoundDefault() {
    RatchetOptions userOptions =
        RatchetOptions.builder()
            .polling(polling -> polling.batchSize(73))
            .security(security -> security.classPolicyAllowedPackages(Set.of("run.ratchet.user")))
            .build();
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.registerBean("userRatchetOptions", RatchetOptions.class, () -> userOptions);
      context.register(RatchetAutoConfiguration.class);
      context.refresh();

      assertSame(userOptions, context.getBean(RatchetOptions.class));
      assertEquals(1, context.getBeanNamesForType(RatchetOptions.class, true, false).length);
    }
  }

  @Test
  void enabledFalseDisablesTheWholeCoreAutoConfiguration() {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context
          .getEnvironment()
          .getPropertySources()
          .addFirst(
              new MapPropertySource("test", Map.of(RatchetProperties.ENABLED_PROPERTY, "false")));
      context.register(RatchetAutoConfiguration.class);
      context.refresh();

      assertEquals(0, context.getBeanNamesForType(RatchetOptions.class, true, false).length);
      assertEquals(0, context.getBeanNamesForType(PayloadSerializer.class, true, false).length);
    }
  }

  private static List<Class<?>> exportedDefaultTypes() {
    return List.of(
        ClassPolicy.class,
        ExecutorProvider.class,
        MetricsCollector.class,
        TracingCollector.class,
        ClusterCoordinator.class,
        ErrorSanitizer.class,
        NodeTagAffinityProvider.class,
        CircuitBreakerConfigProvider.class,
        CircuitBreakerManager.class,
        ResilienceStrategy.class,
        Clock.class,
        JobAuthorizationPolicy.class,
        JobInvocationResolver.class,
        PollingStrategyProvider.class,
        ExecutionTuningProvider.class,
        RetryPolicy.class,
        PayloadSerializer.class);
  }
}
