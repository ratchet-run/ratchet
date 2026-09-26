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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import run.ratchet.api.*;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.*;
import run.ratchet.store.spi.JobStore;

class SpringRecurringDiscoveryTest {
  public interface Contract {
    @Recurring(cron = "0 * * * * ?")
    void run();
  }

  public static class Task implements Contract {
    public void run() {}
  }

  public static class DirectTask {
    @Recurring(cron = "0 * * * * ?")
    public void run() {}
  }

  public interface DefaultContract {
    @Recurring(cron = "0 * * * * ?")
    default void run() {}
  }

  public static class DefaultTask implements DefaultContract {}

  public static class OverriddenTask extends DirectTask {
    @Override
    public void run() {}
  }

  @Test
  void overriddenMethodsAreDiscoveredOnceAndDefaultMethodsAreRetained() throws Exception {
    assertThat(SpringRecurringDiscovery.recurringMethods(OverriddenTask.class).keySet())
        .containsExactly(OverriddenTask.class.getMethod("run"));
    assertThat(SpringRecurringDiscovery.recurringMethods(DefaultTask.class).keySet())
        .containsExactly(DefaultContract.class.getMethod("run"));
  }

  @Test
  void registrationPreservesLazyAndPrototypeBeansAndInterfaceAnnotations() throws Exception {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger created = new AtomicInteger();
      context.registerBean(
          "lazy",
          Task.class,
          () -> {
            created.incrementAndGet();
            return new Task();
          },
          definition -> definition.setLazyInit(true));
      context.registerBean(
          "prototype",
          DirectTask.class,
          () -> {
            created.incrementAndGet();
            return new DirectTask();
          },
          definition -> definition.setScope("prototype"));
      context.registerBean("defaultMethod", DefaultTask.class, DefaultTask::new);
      context.refresh();
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      var invoker = new RecurringMethodInvoker(resolver, name -> true);
      var submissions = mock(InvocationSubmissionService.class);
      var builder = mock(RecurringJobBuilder.class, RETURNS_SELF);
      when(submissions.scheduleRecurringInvocation(anyString(), any(), any())).thenReturn(builder);
      when(builder.submit()).thenReturn((JobHandle) UUID::randomUUID);
      var discovery =
          new SpringRecurringDiscovery(
              context.getBeanFactory(),
              submissions,
              mock(JobStore.class),
              mock(RecurringAnnotationMaintenanceService.class),
              invoker,
              mock(StartupCoordinator.class),
              new RecurringRegistrationState(),
              RatchetOptions.defaults(),
              Clock.systemUTC());
      discovery.register();
      verify(builder, times(3)).submit();
      assertThat(created).hasValue(0);
      invoker.invoke(Task.class.getName(), "run", false);
      invoker.invoke(DirectTask.class.getName(), "run", false);
      invoker.invoke(DirectTask.class.getName(), "run", false);
      assertThat(created).hasValue(3);
    }
  }

  public static class UnexposedTask implements Runnable {
    static final AtomicInteger created = new AtomicInteger();

    public UnexposedTask() {
      created.incrementAndGet();
    }

    @Override
    public void run() {}

    @Recurring(cron = "0 * * * * ?")
    public void unavailable() {}
  }

  @Test
  void eagerUnexposedJdkProxyFailsRegistration() {
    try (var context = new AnnotationConfigApplicationContext()) {
      proxyUnexposedBean(context);
      context.registerBean("unexposed", UnexposedTask.class, UnexposedTask::new);
      context.refresh();
      assertThat(Proxy.isProxyClass(context.getBeanFactory().getSingleton("unexposed").getClass()))
          .isTrue();
      assertThatThrownBy(() -> discovery(context).register())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("cannot invoke @Recurring method through its Spring proxy")
          .hasCauseInstanceOf(NoSuchMethodException.class);
    }
  }

  @Test
  void lazyUnexposedJdkProxyIsNotInstantiatedDuringRegistration() {
    UnexposedTask.created.set(0);
    try (var context = new AnnotationConfigApplicationContext()) {
      proxyUnexposedBean(context);
      context.registerBean(
          "unexposed",
          UnexposedTask.class,
          UnexposedTask::new,
          definition -> definition.setLazyInit(true));
      context.refresh();
      var discovery = discovery(context);
      assertThat(discovery.discover()).containsExactly(UnexposedTask.class);
      discovery.register();
      assertThat(UnexposedTask.created).hasValue(0);
      assertThat(context.getBeanFactory().getSingleton("unexposed")).isNull();
    }
  }

  @Test
  void primaryExposedBeanIsValidatedInsteadOfUnexposedJdkProxy() {
    try (var context = new AnnotationConfigApplicationContext()) {
      proxyUnexposedBean(context);
      context.registerBean("unexposed", UnexposedTask.class, UnexposedTask::new);
      context.registerBean(
          "primary",
          UnexposedTask.class,
          UnexposedTask::new,
          definition -> definition.setPrimary(true));
      context.refresh();
      var discovery = discovery(context);
      assertThat(discovery.discover()).containsExactly(UnexposedTask.class);
      discovery.register();
    }
  }

  private static void proxyUnexposedBean(AnnotationConfigApplicationContext context) {
    context
        .getBeanFactory()
        .addBeanPostProcessor(
            new BeanPostProcessor() {
              @Override
              public Object postProcessAfterInitialization(Object bean, String name) {
                if (!name.equals("unexposed")) return bean;
                var proxy = new ProxyFactory(bean);
                proxy.setProxyTargetClass(false);
                proxy.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                return proxy.getProxy();
              }
            });
  }

  private static SpringRecurringDiscovery discovery(AnnotationConfigApplicationContext context) {
    var submissions = mock(InvocationSubmissionService.class);
    var builder = mock(RecurringJobBuilder.class, RETURNS_SELF);
    when(submissions.scheduleRecurringInvocation(anyString(), any(), any())).thenReturn(builder);
    when(builder.submit()).thenReturn((JobHandle) UUID::randomUUID);
    return new SpringRecurringDiscovery(
        context.getBeanFactory(),
        submissions,
        mock(JobStore.class),
        mock(RecurringAnnotationMaintenanceService.class),
        new RecurringMethodInvoker(new SpringBeanResolver(context.getBeanFactory()), name -> true),
        mock(StartupCoordinator.class),
        new RecurringRegistrationState(),
        RatchetOptions.defaults(),
        Clock.systemUTC());
  }
}
