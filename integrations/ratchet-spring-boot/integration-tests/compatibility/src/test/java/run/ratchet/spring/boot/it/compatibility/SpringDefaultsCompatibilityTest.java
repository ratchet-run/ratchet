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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration;

class SpringDefaultsCompatibilityTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class));

  @Test
  void springPropertiesBindCanonicalOptionsAndUserOptionsBackOffTheDefault() {
    contextRunner
        .withPropertyValues(
            "ratchet.poller.batch-size=37",
            "ratchet.class-policy.allowed-packages=run.ratchet.spring.boot.it.compatibility")
        .run(
            context ->
                assertEquals(37, context.getBean(RatchetOptions.class).polling().batchSize()));

    RatchetOptions userOptions =
        RatchetOptions.builder()
            .polling(polling -> polling.batchSize(19))
            .security(security -> security.allowEmptyClassPolicy(true))
            .build();
    contextRunner
        .withBean(RatchetOptions.class, () -> userOptions)
        .run(context -> assertSame(userOptions, context.getBean(RatchetOptions.class)));
  }

  @Test
  void disabledAutoConfigurationRegistersNoRatchetBeansThreadsOrMigrationInitializer() {
    Set<String> threadNamesBefore = ratchetThreadNames();
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class, RatchetJpaAutoConfiguration.class))
        .withPropertyValues("ratchet.enabled=false")
        .run(
            context -> {
              ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
              for (String beanName : beanFactory.getBeanDefinitionNames()) {
                Class<?> beanType = beanFactory.getType(beanName, false);
                assertFalse(
                    beanType != null && beanType.getName().startsWith("run.ratchet."),
                    () -> "ratchet.enabled=false registered " + beanName + " as " + beanType);
              }
              assertFalse(
                  Arrays.stream(beanFactory.getBeanDefinitionNames())
                      .anyMatch(name -> name.contains("ratchetJpaSchemaMigrationInitializer")));
              assertEquals(threadNamesBefore, ratchetThreadNames());
            });
  }

  @Test
  void classPolicyBindsSeparateAllowlistsAndKeepsTheGadgetDenylist() {
    contextRunner
        .withPropertyValues(
            "ratchet.class-policy.allowed-packages=java.lang",
            "ratchet.class-policy.allowed-result-type-packages=java.util")
        .run(
            context -> {
              ClassPolicy policy = context.getBean(ClassPolicy.class);

              assertTrue(policy.isAllowed("java.lang.String"));
              assertFalse(policy.isAllowed("java.lang.Runtime"));
              assertFalse(policy.isAllowedForResultType("java.lang.String"));
              assertTrue(policy.isAllowedForResultType("java.util.ArrayList"));
            });
  }

  @Test
  void emptyClassPolicyFailsWithActionableMessageUnlessExplicitlyAllowed() {
    contextRunner.run(
        context -> {
          Throwable failure = context.getStartupFailure();
          assertNotNull(failure);
          assertTrue(
              causalMessages(failure)
                  .contains(
                      "ClassPolicy invocation allowlist is empty - refusing to start. Configure"
                          + " ratchet.class-policy.allowed-packages or set"
                          + " ratchet.allow-empty-class-policy=true ONLY for demos/tests."));
        });

    contextRunner
        .withPropertyValues("ratchet.allow-empty-class-policy=true")
        .run(context -> assertNotNull(context.getBean(ClassPolicy.class)));
  }

  @Test
  void standaloneExecutorsAreLazyNonJndiDefaultsAndOwnedPoolsCloseWithTheContext() {
    AtomicReference<ExecutorService> jobExecutor = new AtomicReference<>();
    AtomicReference<ScheduledExecutorService> scheduledExecutor = new AtomicReference<>();

    contextRunner
        .withPropertyValues("ratchet.allow-empty-class-policy=true")
        .run(
            context -> {
              ExecutorProvider provider = context.getBean(ExecutorProvider.class);
              assertTrue(provider instanceof StandaloneExecutorProvider);
              jobExecutor.set(provider.getJobExecutor());
              scheduledExecutor.set(provider.getScheduledExecutor());
              assertFalse(jobExecutor.get().isShutdown());
              assertFalse(scheduledExecutor.get().isShutdown());
            });

    assertTrue(jobExecutor.get().isShutdown());
    assertTrue(scheduledExecutor.get().isShutdown());
  }

  @Test
  void userExecutorProviderIsNeitherReplacedNorClosedByRatchet() {
    ExecutorService jobExecutor = mock(ExecutorService.class);
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    ExecutorProvider userProvider = mock(ExecutorProvider.class);
    when(userProvider.getJobExecutor()).thenReturn(jobExecutor);
    when(userProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);

    contextRunner
        .withPropertyValues("ratchet.allow-empty-class-policy=true")
        .withBean(ExecutorProvider.class, () -> userProvider)
        .run(
            context -> {
              assertSame(userProvider, context.getBean(ExecutorProvider.class));
              context.getBean(ExecutorProvider.class).getJobExecutor();
              context.getBean(ExecutorProvider.class).getScheduledExecutor();
            });

    verify(jobExecutor, never()).shutdown();
    verify(jobExecutor, never()).shutdownNow();
    verify(scheduledExecutor, never()).shutdown();
    verify(scheduledExecutor, never()).shutdownNow();
  }

  private static Set<String> ratchetThreadNames() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .map(Thread::getName)
        .filter(name -> name.startsWith("ratchet-"))
        .collect(Collectors.toSet());
  }

  private static String causalMessages(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        messages.append(current.getMessage()).append('\n');
      }
    }
    return messages.toString();
  }
}
