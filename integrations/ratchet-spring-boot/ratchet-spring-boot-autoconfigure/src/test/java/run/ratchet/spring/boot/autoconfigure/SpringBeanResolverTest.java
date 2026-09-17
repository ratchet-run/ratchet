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

import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicInteger;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import run.ratchet.ri.core.internal.ManagedInvocation;

class SpringBeanResolverTest {
  interface Task {
    void run();
  }

  static class TaskBean implements Task {
    final AtomicInteger executions = new AtomicInteger();
    final AtomicInteger destructions;

    TaskBean(AtomicInteger destructions) {
      this.destructions = destructions;
    }

    @Override
    public void run() {
      executions.incrementAndGet();
    }

    public void unavailable() {}

    @PreDestroy
    public void destroy() {
      destructions.incrementAndGet();
    }
  }

  @Test
  void concreteLookupRetainsJdkProxyAdvice() throws Exception {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger advice = new AtomicInteger();
      TaskBean target = new TaskBean(new AtomicInteger());
      var proxy = new ProxyFactory(target);
      proxy.addAdvice(
          (MethodInterceptor)
              invocation -> {
                advice.incrementAndGet();
                return invocation.proceed();
              });
      context.registerBean("task", Task.class, () -> (Task) proxy.getProxy());
      context.refresh();
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var bean = resolver.acquire(TaskBean.class)) {
        ManagedInvocation.exposedMethod(TaskBean.class.getMethod("run"), bean.instance())
            .invoke(bean.instance());
        assertThat(advice).hasValue(1);
        assertThat(target.executions).hasValue(1);
        assertThatThrownBy(
                () ->
                    ManagedInvocation.exposedMethod(
                        TaskBean.class.getMethod("unavailable"), bean.instance()))
            .hasMessageContaining("does not expose");
      }
    }
  }

  @Test
  void prototypeClassProxyIsDestroyedOnce() throws Exception {
    prototypeDestroyed(true);
  }

  @Test
  void prototypeInterfaceProxyIsDestroyedOnce() throws Exception {
    prototypeDestroyed(false);
  }

  private void prototypeDestroyed(boolean classProxy) throws Exception {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      AtomicInteger advice = new AtomicInteger();
      context
          .getBeanFactory()
          .addBeanPostProcessor(
              new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String name) {
                  if (!(bean instanceof TaskBean)) return bean;
                  var proxy = new ProxyFactory(bean);
                  proxy.setProxyTargetClass(classProxy);
                  proxy.addAdvice(
                      (MethodInterceptor)
                          invocation -> {
                            advice.incrementAndGet();
                            return invocation.proceed();
                          });
                  return proxy.getProxy();
                }
              });
      context.registerBean(
          "task",
          TaskBean.class,
          () -> new TaskBean(destructions),
          definition -> definition.setScope("prototype"));
      context.refresh();
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      var bean = resolver.acquire(TaskBean.class);
      ManagedInvocation.exposedMethod(TaskBean.class.getMethod("run"), bean.instance())
          .invoke(bean.instance());
      assertThat(advice).hasValue(1);
      bean.close();
      bean.close();
      assertThat(destructions).hasValue(1);
    }
  }

  @Test
  void primaryDisambiguatesAndSingletonIsNotDestroyedByHandle() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      context.registerBean("first", TaskBean.class, () -> new TaskBean(destructions));
      context.registerBean(
          "second",
          TaskBean.class,
          () -> new TaskBean(destructions),
          definition -> definition.setPrimary(true));
      context.refresh();
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isSameAs(context.getBean("second"));
      }
      assertThat(destructions).hasValue(0);
    }
  }
}
