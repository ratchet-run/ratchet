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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.Order;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.CallerPrincipalResolution;
import run.ratchet.spi.CallerPrincipalResolver;
import run.ratchet.spi.PrincipalSource;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

class SpringPrincipalCompatibilityTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
          .withPropertyValues("ratchet.allow-empty-class-policy=true");

  @Test
  void springSourcesUseOrderAndReturnTheFirstNonEmptyPrincipal() {
    contextRunner
        .withUserConfiguration(OrderedSources.class)
        .run(
            context ->
                assertEquals(
                    Optional.of("alice"),
                    context.getBean(CallerPrincipalProvider.class).currentPrincipal()));
  }

  @Test
  void throwingAndNullSourcesDegradeAndContinueToALaterSource() {
    List<PrincipalSource> sources = new ArrayList<>();
    sources.add(
        () -> {
          throw new IllegalStateException("request context is unavailable");
        });
    sources.add(null);
    sources.add(() -> Optional.of("fallback"));

    assertEquals(Optional.of("fallback"), new CallerPrincipalProvider(sources).currentPrincipal());
  }

  @Test
  void springSourceIsEvaluatedOnEveryCall() {
    contextRunner
        .withUserConfiguration(DynamicSourceConfiguration.class)
        .run(
            context -> {
              MutablePrincipalSource source = context.getBean(MutablePrincipalSource.class);
              CallerPrincipalProvider provider = context.getBean(CallerPrincipalProvider.class);

              source.principal.set("first");
              assertEquals(Optional.of("first"), provider.currentPrincipal());
              source.principal.set("second");
              assertEquals(Optional.of("second"), provider.currentPrincipal());
            });
  }

  @Test
  void configuredResolverThenNestedJobContextThenSpringSourceDefinePrecedence() {
    CallerPrincipalProvider springProvider =
        new CallerPrincipalProvider(List.of(() -> Optional.of("spring")));
    CallerPrincipalResolver configured = () -> Optional.of("configured");
    CallerPrincipalResolver empty = Optional::empty;

    JobContext.bind(UUID.randomUUID(), null, Map.of(), "parent", null);
    try {
      assertEquals(
          Optional.of("configured"), CallerPrincipalResolution.resolve(configured, springProvider));
      assertEquals(Optional.of("parent"), CallerPrincipalResolution.resolve(empty, springProvider));
    } finally {
      JobContext.clear();
    }

    assertEquals(Optional.of("spring"), CallerPrincipalResolution.resolve(empty, springProvider));
  }

  @Test
  void nestedSubmissionThroughSpringSchedulerPersistsTheParentPrincipal() {
    JobStore store = mock(JobStore.class);
    AtomicReference<JobEntity> created = new AtomicReference<>();
    when(store.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(store.create(any(JobEntity.class)))
        .thenAnswer(
            invocation -> {
              JobEntity entity = invocation.getArgument(0);
              entity.setId(UUID.randomUUID());
              created.set(entity);
              return entity;
            });

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
        .withInitializer(
            context ->
                ((GenericApplicationContext) context)
                    .registerBean("principalTestJobStore", JobStore.class, () -> store))
        .withUserConfiguration(OrderedSources.class)
        .withPropertyValues(
            "ratchet.class-policy.allowed-packages=run.ratchet.spring.boot.it.compatibility")
        .run(
            context -> {
              JobContext.bind(UUID.randomUUID(), null, Map.of(), "parent", null);
              try {
                JobHandle child =
                    context
                        .getBean(JobSchedulerService.class)
                        .schedule(Duration.ofMinutes(1), SpringPrincipalCompatibilityTest::noopTask)
                        .submit();

                JobEntity persisted = created.get();
                assertNotNull(persisted);
                assertEquals(persisted.getId(), child.id());
                assertEquals(
                    "parent",
                    persisted.getCallerPrincipal(),
                    "A nested submission must inherit JobContext before consulting Spring sources");
              } finally {
                JobContext.clear();
              }
            });
  }

  @Test
  void nullAndThrowingResolversFallThroughAndAllEmptySourcesStayEmpty() {
    CallerPrincipalResolver nullResolver = () -> null;
    CallerPrincipalResolver throwingResolver =
        () -> {
          throw new IllegalStateException("request scope is inactive");
        };
    CallerPrincipalProvider provider =
        new CallerPrincipalProvider(List.of(() -> Optional.of("spring")));

    assertEquals(Optional.of("spring"), CallerPrincipalResolution.resolve(nullResolver, provider));
    assertEquals(
        Optional.of("spring"), CallerPrincipalResolution.resolve(throwingResolver, provider));
    assertTrue(
        CallerPrincipalResolution.resolve(Optional::empty, new CallerPrincipalProvider(List.of()))
            .isEmpty());
  }

  @Configuration(proxyBeanMethods = false)
  static class OrderedSources {

    @Bean
    @Order(0)
    PrincipalSource emptySource() {
      return Optional::empty;
    }

    @Bean
    @Order(10)
    PrincipalSource firstSource() {
      return () -> Optional.of("alice");
    }

    @Bean
    @Order(20)
    PrincipalSource laterSource() {
      return () -> Optional.of("bob");
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class DynamicSourceConfiguration {

    @Bean
    MutablePrincipalSource mutablePrincipalSource() {
      return new MutablePrincipalSource();
    }
  }

  static final class MutablePrincipalSource implements PrincipalSource {

    private final AtomicReference<String> principal = new AtomicReference<>();

    @Override
    public Optional<String> currentPrincipal() {
      return Optional.ofNullable(principal.get());
    }
  }

  public static void noopTask() {}
}
