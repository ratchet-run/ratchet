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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
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

  private static final ResolvableType SCHEDULED_EXECUTOR_SUPPLIER_TYPE =
      ResolvableType.forClassWithGenerics(Supplier.class, ScheduledExecutorService.class);

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
  void scheduledExecutorSupplierUsesProviderAndExecutorIsDisposedOnContextClose() {
    ScheduledExecutorService scheduledExecutor;
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      allowEmptyClassPolicy(context);
      context.register(RatchetAutoConfiguration.class);
      context.refresh();

      String[] beanNames = context.getBeanNamesForType(SCHEDULED_EXECUTOR_SUPPLIER_TYPE);
      assertEquals(1, beanNames.length);
      assertEquals("ratchetScheduledExecutorSupplier", beanNames[0]);

      Supplier<?> supplier =
          (Supplier<?>) context.getBeanProvider(SCHEDULED_EXECUTOR_SUPPLIER_TYPE).getObject();
      scheduledExecutor = (ScheduledExecutorService) supplier.get();
      assertNotNull(scheduledExecutor);
      assertSame(scheduledExecutor, context.getBean(ExecutorProvider.class).getScheduledExecutor());
    }

    assertTrue(scheduledExecutor.isShutdown());
  }

  @Test
  void userScheduledExecutorSupplierBacksOffTheGenericDefault() {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      allowEmptyClassPolicy(context);
      context.register(
          UserScheduledExecutorSupplierConfiguration.class, RatchetAutoConfiguration.class);
      context.refresh();

      String[] beanNames = context.getBeanNamesForType(SCHEDULED_EXECUTOR_SUPPLIER_TYPE);
      assertEquals(1, beanNames.length);
      assertEquals("userScheduledExecutorSupplier", beanNames[0]);
    }
  }

  @Test
  void lifecycleDrainTimeoutBindsWithThirtySecondDefault() {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context
          .getEnvironment()
          .getPropertySources()
          .addFirst(
              new MapPropertySource(
                  "test",
                  Map.of(
                      RatchetProperties.LIFECYCLE_DRAIN_TIMEOUT_PROPERTY,
                      "PT7S",
                      "ratchet.allow-empty-class-policy",
                      "true")));
      context.register(RatchetAutoConfiguration.class);
      context.refresh();

      assertEquals(
          Duration.ofSeconds(7),
          context.getBean(RatchetProperties.class).getLifecycle().getDrainTimeout());
    }

    assertEquals(Duration.ofSeconds(30), new RatchetProperties().getLifecycle().getDrainTimeout());
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
      assertEquals(0, context.getBeanNamesForType(RatchetLifecycle.class, true, false).length);
    }
  }

  private static void allowEmptyClassPolicy(AnnotationConfigApplicationContext context) {
    context
        .getEnvironment()
        .getPropertySources()
        .addFirst(
            new MapPropertySource("test", Map.of("ratchet.allow-empty-class-policy", "true")));
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

  @Configuration(proxyBeanMethods = false)
  static class UserScheduledExecutorSupplierConfiguration {

    @Bean
    Supplier<ScheduledExecutorService> userScheduledExecutorSupplier() {
      return () -> null;
    }
  }
}
