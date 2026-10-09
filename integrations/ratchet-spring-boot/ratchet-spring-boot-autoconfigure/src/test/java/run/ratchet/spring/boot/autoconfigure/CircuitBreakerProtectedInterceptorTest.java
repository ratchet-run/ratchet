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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.aop.Advisor;
import org.springframework.aop.support.AopUtils;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.Ordered;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.NoOpMetricsCollector;

class CircuitBreakerProtectedInterceptorTest {
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetCircuitBreakerAutoConfiguration.class))
          .withUserConfiguration(Services.class);

  @Test
  void methodClassInheritedAndDefaultNamesAreProtected() {
    runner
        .withUserConfiguration(Policies.class)
        .run(
            context -> {
              var service = context.getBean(ProtectedService.class);
              assertThat(AopUtils.isAopProxy(service)).isTrue();
              open(service::fail);
              assertThrows(CircuitBreakerOpenException.class, service::fail);
              open(service::defaultCall);
              assertThat(assertThrows(CircuitBreakerOpenException.class, service::defaultCall))
                  .hasMessageContaining("ProtectedService.defaultCall");
              var classService = context.getBean("classService", ClassService.class);
              open(classService::fail);
              assertThrows(CircuitBreakerOpenException.class, classService::fail);
              var inherited = context.getBean(InheritedService.class);
              assertThat(AopUtils.isAopProxy(inherited)).isTrue();
              // Reset shared state so the inherited binding must open the circuit itself.
              context
                  .getBean(CircuitBreakerRegistry.class)
                  .getBreaker("class", CircuitBreakerProfile.DEFAULT)
                  .reset();
              open(inherited::fail);
              assertThrows(CircuitBreakerOpenException.class, inherited::fail);
            });
  }

  @Test
  void methodAttributesOverrideClassAttributesAndIgnoreWins() {
    runner
        .withUserConfiguration(Policies.class)
        .run(
            context -> {
              var service = context.getBean("classService", ClassService.class);
              for (int i = 0; i < 10; i++)
                assertThrows(IllegalArgumentException.class, service::ignored);
              var registry = context.getBean(CircuitBreakerRegistry.class);
              assertThat(registry.getBreaker("override", CircuitBreakerProfile.FAST).getState())
                  .isEqualTo(CircuitBreaker.State.CLOSED);
              assertThat(service.success()).isEqualTo("ok");
              open(service::override);
              assertThrows(CircuitBreakerOpenException.class, service::ignored);
              assertThat(registry.getBreaker("class", CircuitBreakerProfile.DEFAULT).getState())
                  .isEqualTo(CircuitBreaker.State.CLOSED);
            });
  }

  @Test
  void classFilterExcludesUnrecordedFailures() {
    runner
        .withUserConfiguration(Policies.class)
        .run(
            context -> {
              var service = context.getBean("classService", ClassService.class);
              for (int i = 0; i < 10; i++)
                assertThrows(UnsupportedOperationException.class, service::unrecorded);
              assertThat(
                      context
                          .getBean(CircuitBreakerRegistry.class)
                          .getBreaker("class", CircuitBreakerProfile.DEFAULT)
                          .getState())
                  .isEqualTo(CircuitBreaker.State.CLOSED);
              open(service::fail);
              assertThrows(CircuitBreakerOpenException.class, service::success);
            });
  }

  @Test
  void annotationMembersHaveNativeReflectionHints() {
    var hints = new RuntimeHints();
    new RatchetRuntimeHints().registerHints(hints, getClass().getClassLoader());
    // Read the registered hint directly: the predicate API differs between Spring 6 and 7.
    var type = hints.reflection().getTypeHint(CircuitBreakerProtected.class);
    assertThat(type).isNotNull();
    for (var method : CircuitBreakerProtected.class.getDeclaredMethods()) {
      assertThat(type.methods())
          .anyMatch(
              hint ->
                  hint.getName().equals(method.getName())
                      && hint.getMode() == ExecutableMode.INVOKE);
    }
  }

  @Test
  void recordedErrorsTripAndCheckedExceptionsRetainIdentity() {
    runner
        .withUserConfiguration(Policies.class)
        .run(
            context -> {
              var service = context.getBean(ProtectedService.class);
              for (int i = 0; i < 3; i++) {
                assertSame(
                    ProtectedService.ERROR, assertThrows(AssertionError.class, service::error));
              }
              assertThrows(CircuitBreakerOpenException.class, service::error);
              assertSame(ProtectedService.CHECKED, assertThrows(Exception.class, service::checked));
            });
  }

  @Test
  void disabledProviderPassesThroughWithoutCreatingRegistry() {
    runner
        .withBean(CircuitBreakerConfigProvider.class, () -> new TestPolicy(false))
        .withBean(
            CircuitBreakerRegistry.class,
            () -> {
              throw new AssertionError("disabled advice resolved registry");
            },
            definition -> definition.setLazyInit(true))
        .run(
            context -> {
              var service = context.getBean(ProtectedService.class);
              for (int i = 0; i < 5; i++) assertThrows(IllegalStateException.class, service::fail);
            });
  }

  @Test
  void absentPoliciesAndAbsentRegistryPassThrough() {
    runner
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class, RatchetEngineAutoConfiguration.class))
        .withPropertyValues("ratchet.enabled=false")
        .run(
            context -> {
              var service = context.getBean(ProtectedService.class);
              for (int i = 0; i < 5; i++) assertThrows(IllegalStateException.class, service::fail);
            });
    runner
        .withBean(CircuitBreakerConfigProvider.class, () -> new TestPolicy(true))
        .run(
            context -> {
              var service = context.getBean(ProtectedService.class);
              for (int i = 0; i < 5; i++) assertThrows(IllegalStateException.class, service::fail);
            });
  }

  @Test
  void infrastructureDoesNotEagerlyResolvePolicies() {
    runner
        .withUserConfiguration(Policies.class)
        .run(
            context -> {
              assertThat(
                      context
                          .getSourceApplicationContext()
                          .getBeanFactory()
                          .containsSingleton("policy"))
                  .isFalse();
              assertThat(
                      context
                          .getSourceApplicationContext()
                          .getBeanFactory()
                          .containsSingleton("registry"))
                  .isFalse();
              assertThat(
                      context
                          .getSourceApplicationContext()
                          .getBeanFactory()
                          .getBeanDefinition("ratchetCircuitBreakerAdvisor")
                          .getRole())
                  .isEqualTo(BeanDefinition.ROLE_INFRASTRUCTURE);
              assertThat(
                      ((Ordered) context.getBean("ratchetCircuitBreakerAdvisor", Advisor.class))
                          .getOrder())
                  .isEqualTo(Ordered.HIGHEST_PRECEDENCE + 100);
            });
  }

  @Test
  void bootCreatorAndDisabledBootAopBothProtectBeans() {
    for (String automatic : new String[] {"true", "false"}) {
      runner
          .withConfiguration(AutoConfigurations.of(AopAutoConfiguration.class))
          .withPropertyValues("spring.aop.auto=" + automatic)
          .withUserConfiguration(Policies.class)
          .run(
              context -> {
                var service = context.getBean(ProtectedService.class);
                assertThat(AopUtils.isCglibProxy(service)).isTrue();
                open(service::fail);
                assertThrows(CircuitBreakerOpenException.class, service::fail);
              });
    }
  }

  @Test
  void interfaceInvocationResolvesImplementationAndHonorsJdkProxySetting() {
    runner
        .withPropertyValues("spring.aop.proxy-target-class=false")
        .withUserConfiguration(Policies.class)
        .run(
            context -> {
              var service = context.getBean(ServiceApi.class);
              assertThat(AopUtils.isJdkDynamicProxy(service)).isTrue();
              open(service::call);
              assertThrows(CircuitBreakerOpenException.class, service::call);
              assertThat(
                      context
                          .getBean(CircuitBreakerRegistry.class)
                          .getBreaker("implementation", CircuitBreakerProfile.FAST)
                          .getState())
                  .isEqualTo(CircuitBreaker.State.OPEN);
            });
  }

  @Test
  void openCircuitRejectsBeforeTransactionBegins() {
    runner
        .withUserConfiguration(Policies.class, Transactions.class)
        .run(
            context -> {
              var service = context.getBean(ProtectedService.class);
              var manager = context.getBean(CountingTransactions.class);
              open(service::transactional);
              assertThat(manager.starts.get()).isEqualTo(3);
              assertThrows(CircuitBreakerOpenException.class, service::transactional);
              assertThat(manager.starts.get()).isEqualTo(3);
              assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            });
  }

  private static void open(Runnable call) {
    for (int i = 0; i < 3; i++) assertThrows(IllegalStateException.class, call::run);
  }

  @Configuration(proxyBeanMethods = false)
  static class Services {
    @Bean
    ProtectedService protectedService() {
      return new ProtectedService();
    }

    @Bean
    ClassService classService() {
      return new ClassService();
    }

    @Bean
    InheritedService inheritedService() {
      return new InheritedService();
    }

    @Bean
    ServiceApi serviceApi() {
      return new ServiceImplementation();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class Policies {
    @Bean
    @Lazy
    TestPolicy policy() {
      return new TestPolicy(true);
    }

    @Bean
    @Lazy
    CircuitBreakerRegistry registry(CircuitBreakerConfigProvider policy) {
      return new CircuitBreakerRegistry(policy, new NoOpMetricsCollector());
    }
  }

  static class TestPolicy implements CircuitBreakerConfigProvider {
    private final boolean enabled;

    TestPolicy(boolean enabled) {
      this.enabled = enabled;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
      return new CircuitBreakerConfig(50, 20, 30000, 2, 3);
    }
  }

  public static class ProtectedService {
    static final AssertionError ERROR = new AssertionError("recorded");
    static final Exception CHECKED = new Exception("checked");

    @CircuitBreakerProtected(service = "method")
    public void fail() {
      throw new IllegalStateException();
    }

    @CircuitBreakerProtected
    public void defaultCall() {
      throw new IllegalStateException();
    }

    @CircuitBreakerProtected(service = "error", recordExceptions = AssertionError.class)
    public void error() {
      throw ERROR;
    }

    @CircuitBreakerProtected(service = "checked")
    public void checked() throws Exception {
      throw CHECKED;
    }

    @Transactional
    @CircuitBreakerProtected(service = "transaction")
    public void transactional() {
      assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
      throw new IllegalStateException();
    }
  }

  @CircuitBreakerProtected(service = "class", recordExceptions = IllegalStateException.class)
  public static class ClassService {
    public void fail() {
      throw new IllegalStateException();
    }

    public String success() {
      return "ok";
    }

    public void unrecorded() {
      throw new UnsupportedOperationException();
    }

    @CircuitBreakerProtected(
        service = "override",
        profile = CircuitBreakerProfile.FAST,
        recordExceptions = RuntimeException.class,
        ignoreExceptions = IllegalArgumentException.class)
    public void ignored() {
      throw new IllegalArgumentException();
    }

    @CircuitBreakerProtected(service = "override", profile = CircuitBreakerProfile.FAST)
    public void override() {
      throw new IllegalStateException();
    }
  }

  public static class InheritedService extends ClassService {
    @Override
    public void fail() {
      throw new IllegalStateException();
    }
  }

  interface ServiceApi {
    void call();
  }

  public static class ServiceImplementation implements ServiceApi {
    @Override
    @CircuitBreakerProtected(service = "implementation", profile = CircuitBreakerProfile.FAST)
    public void call() {
      throw new IllegalStateException();
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  static class Transactions {
    @Bean
    CountingTransactions transactionManager() {
      return new CountingTransactions();
    }
  }

  static class CountingTransactions extends AbstractPlatformTransactionManager {
    final AtomicInteger starts = new AtomicInteger();

    @Override
    protected Object doGetTransaction() {
      return new Object();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
      starts.incrementAndGet();
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {}

    @Override
    protected void doRollback(DefaultTransactionStatus status) {}
  }
}
