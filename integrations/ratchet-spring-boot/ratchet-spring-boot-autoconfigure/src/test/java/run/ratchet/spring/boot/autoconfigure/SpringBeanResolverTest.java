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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.DestructionAwareBeanPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
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
  void lookupCachesNamesWithoutCreatingUnrelatedFactoryBeans() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger created = new AtomicInteger();
      context.registerBean(
          "task",
          TaskBean.class,
          () -> new TaskBean(new AtomicInteger()),
          d -> d.setScope("prototype"));
      context.registerBean(
          "factory",
          UnknownFactory.class,
          () -> {
            created.incrementAndGet();
            return new UnknownFactory();
          },
          d -> d.setLazyInit(true));
      context.refresh();
      var factory = org.mockito.Mockito.spy(context.getBeanFactory());
      var resolver = new SpringBeanResolver(factory);
      resolver.validateResolvable(TaskBean.class);
      try (var first = resolver.acquire(TaskBean.class);
          var second = resolver.acquire(TaskBean.class)) {
        assertThat(first.instance()).isNotSameAs(second.instance());
      }
      org.mockito.Mockito.verify(factory).getBeanDefinitionNames();
      assertThat(created).hasValue(0);
    }
  }

  @Test
  void manuallyRegisteredSingletonJobIsResolvableAndBorrowed() {
    try (var context = new AnnotationConfigApplicationContext()) {
      TaskBean task = new TaskBean(new AtomicInteger());
      context.getBeanFactory().registerSingleton("manualTask", task);
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      assertThat(resolver.resolve(TaskBean.class)).isSameAs(task);
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isSameAs(task);
      }
      assertThat(task.destructions).hasValue(0);
    }
  }

  @Test
  void manuallyRegisteredFactoryProductsFollowTheirIndependentOwnershipContract() {
    try (var independentContext = new AnnotationConfigApplicationContext();
        var pooledContext = new AnnotationConfigApplicationContext()) {
      AtomicInteger independentDestructions = new AtomicInteger();
      ProductTaskFactory independent = new ProductTaskFactory(independentDestructions);
      independentContext.getBeanFactory().registerSingleton("manualIndependent", independent);
      independentContext.refresh();

      var independentResolver = new SpringBeanResolver(independentContext.getBeanFactory());
      try (var handle = independentResolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isInstanceOf(TaskBean.class);
      }
      assertThat(independentDestructions).hasValue(1);
      assertThat(independent.products).hasValue(1);

      AtomicInteger pooledDestructions = new AtomicInteger();
      NonIndependentTaskFactory pooled = new NonIndependentTaskFactory(pooledDestructions);
      pooledContext.getBeanFactory().registerSingleton("manualPooled", pooled);
      pooledContext.refresh();

      var pooledResolver = new SpringBeanResolver(pooledContext.getBeanFactory());
      assertThat(pooledResolver.resolve(TaskBean.class)).isSameAs(pooled.product);
      try (var handle = pooledResolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isSameAs(pooled.product);
      }
      assertThat(pooledDestructions).hasValue(0);
    }
  }

  @Test
  void manuallyRegisteredIndependentFactoryProductClosesAfterSingletonDestruction() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      ProductTaskFactory factory = new ProductTaskFactory(destructions);
      context.getBeanFactory().registerSingleton("manualIndependent", factory);
      context.refresh();

      var handle = SpringManagedBeans.acquire(context.getBeanFactory(), "manualIndependent");
      context.getBeanFactory().destroySingletons();
      handle.close();
      assertThat(destructions).hasValue(1);
    }
  }

  static class UnknownFactory implements org.springframework.beans.factory.FactoryBean<Object> {
    public Object getObject() {
      return new Object();
    }

    public Class<?> getObjectType() {
      return null;
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

  @Test
  void acquireIgnoresASecondNonAutowireCandidateLikeSpringTypeLookup() {
    try (var context = new AnnotationConfigApplicationContext()) {
      TaskBean regular = new TaskBean(new AtomicInteger());
      context.registerBean("regular", TaskBean.class, () -> regular);
      context.registerBean(
          "hidden",
          TaskBean.class,
          () -> new TaskBean(new AtomicInteger()),
          definition -> definition.setAutowireCandidate(false));
      context.refresh();

      TaskBean springSelected = context.getBean(TaskBean.class);
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(springSelected).isSameAs(regular);
        assertThat(handle.instance()).isSameAs(springSelected);
      }
    }
  }

  @Test
  void acquireIgnoresAHiddenPrimaryCandidateLikeSpringTypeLookup() {
    try (var context = new AnnotationConfigApplicationContext()) {
      TaskBean regular = new TaskBean(new AtomicInteger());
      context.registerBean("regular", TaskBean.class, () -> regular);
      context.registerBean(
          "hidden",
          TaskBean.class,
          () -> new TaskBean(new AtomicInteger()),
          definition -> {
            definition.setAutowireCandidate(false);
            definition.setPrimary(true);
          });
      context.refresh();

      TaskBean springSelected = context.getBean(TaskBean.class);
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(springSelected).isSameAs(regular);
        assertThat(handle.instance()).isSameAs(springSelected);
      }
    }
  }

  @Test
  void acquireRetainsTheSoleNonAutowireCandidateLikeSpringTypeLookup() {
    try (var context = new AnnotationConfigApplicationContext()) {
      TaskBean hidden = new TaskBean(new AtomicInteger());
      context.registerBean(
          "hidden",
          TaskBean.class,
          () -> hidden,
          definition -> definition.setAutowireCandidate(false));
      context.refresh();

      TaskBean springSelected = context.getBean(TaskBean.class);
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(springSelected).isSameAs(hidden);
        assertThat(handle.instance()).isSameAs(springSelected);
      }
    }
  }

  @Test
  void acquireRetainsAllNonAutowireCandidatesForPrimarySelectionLikeSpringTypeLookup() {
    try (var context = new AnnotationConfigApplicationContext()) {
      TaskBean first = new TaskBean(new AtomicInteger());
      TaskBean second = new TaskBean(new AtomicInteger());
      context.registerBean(
          "first",
          TaskBean.class,
          () -> first,
          definition -> definition.setAutowireCandidate(false));
      context.registerBean(
          "second",
          TaskBean.class,
          () -> second,
          definition -> {
            definition.setAutowireCandidate(false);
            definition.setPrimary(true);
          });
      context.refresh();

      TaskBean springSelected = context.getBean(TaskBean.class);
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(springSelected).isSameAs(second);
        assertThat(handle.instance()).isSameAs(springSelected);
      }
    }
  }

  @Test
  void acquireSelectsTheSoleNonFallbackBeanLikeSpringTypeLookup() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      context.registerBean("preferred", TaskBean.class, () -> new TaskBean(destructions));
      context.registerBean("fallback", TaskBean.class, () -> new TaskBean(destructions));
      context.getBeanFactory().getBeanDefinition("fallback").setFallback(true);
      context.refresh();

      TaskBean springSelected = context.getBean(TaskBean.class);
      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(springSelected).isSameAs(context.getBean("preferred"));
        assertThat(handle.instance()).isSameAs(springSelected);
      }
    }
  }

  @Test
  void factoryBeanProductUsesProductLifecycleInsteadOfFactoryDestroyMetadata() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      ProductTaskFactory factory = new ProductTaskFactory(destructions);
      context.registerBean(
          "factoryProduct",
          ProductTaskFactory.class,
          () -> factory,
          definition -> definition.setDestroyMethodName("shutdownFactory"));
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isInstanceOf(TaskBean.class);
        assertThat(factory.products).hasValue(1);
      }
      assertThat(destructions).hasValue(1);
      assertThat(factory.products).hasValue(1);
      assertThat(factory.shutdowns).hasValue(0);
    }
  }

  @Test
  void ordinaryAndDereferencedPrototypeFactoriesKeepNamedDestroyMetadata() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger ordinaryClosed = new AtomicInteger();
      AtomicReference<ProductTaskFactory> dereferencedFactory = new AtomicReference<>();
      context.registerBean(
          "ordinary",
          NamedDestroyTask.class,
          () -> new NamedDestroyTask(ordinaryClosed),
          definition -> {
            definition.setScope("prototype");
            definition.setDestroyMethodName("closeTask");
          });
      context.registerBean(
          "prototypeFactory",
          ProductTaskFactory.class,
          () -> {
            var factory = new ProductTaskFactory(new AtomicInteger());
            dereferencedFactory.set(factory);
            return factory;
          },
          definition -> {
            definition.setScope("prototype");
            definition.setDestroyMethodName("shutdownFactory");
          });
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var ordinary = resolver.acquire(NamedDestroyTask.class)) {
        assertThat(ordinary.instance()).isInstanceOf(NamedDestroyTask.class);
      }
      try (var factory = resolver.acquire(ProductTaskFactory.class)) {
        assertThat(factory.instance()).isSameAs(dereferencedFactory.get());
      }
      assertThat(ordinaryClosed).hasValue(1);
      assertThat(dereferencedFactory.get().shutdowns).hasValue(1);
    }
  }

  @Test
  void pooledFactoryProductRemainsSpringOwned() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      PooledTaskFactory factory = new PooledTaskFactory(destructions);
      context.registerBean("pooledProduct", PooledTaskFactory.class, () -> factory);
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isSameAs(factory.product);
      }
      assertThat(destructions).hasValue(0);
    }
  }

  @Test
  void nonIndependentSmartFactoryProductRemainsFactoryOwned() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      NonIndependentTaskFactory factory = new NonIndependentTaskFactory(destructions);
      context.registerBean(
          "nonIndependentProduct",
          NonIndependentTaskFactory.class,
          () -> factory,
          definition -> definition.setLazyInit(true));
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      assertThat(context.getBeanFactory().getSingleton("nonIndependentProduct")).isNull();
      TaskBean first;
      try (var handle =
          SpringManagedBeans.acquire(context.getBeanFactory(), "nonIndependentProduct")) {
        first = (TaskBean) handle.instance();
      }
      assertThat(destructions).hasValue(0);
      assertThat(resolver.resolve(TaskBean.class)).isSameAs(first);
      try (var handle = resolver.acquire(TaskBean.class)) {
        assertThat(handle.instance()).isSameAs(first);
      }
      assertThat(destructions).hasValue(0);
    }
  }

  static class NamedDestroyTask {
    final AtomicInteger closed;

    NamedDestroyTask(AtomicInteger closed) {
      this.closed = closed;
    }

    public void closeTask() {
      closed.incrementAndGet();
    }
  }

  static class ProductTaskFactory
      implements org.springframework.beans.factory.SmartFactoryBean<TaskBean> {
    final AtomicInteger destructions;
    final AtomicInteger products = new AtomicInteger();
    final AtomicInteger shutdowns = new AtomicInteger();

    ProductTaskFactory(AtomicInteger destructions) {
      this.destructions = destructions;
    }

    @Override
    public TaskBean getObject() {
      products.incrementAndGet();
      return new TaskBean(destructions);
    }

    @Override
    public Class<?> getObjectType() {
      return TaskBean.class;
    }

    @Override
    public boolean isSingleton() {
      return false;
    }

    @Override
    public boolean isPrototype() {
      return true;
    }

    public void shutdownFactory() {
      shutdowns.incrementAndGet();
    }
  }

  static class PooledTaskFactory
      implements org.springframework.beans.factory.SmartFactoryBean<TaskBean> {
    final TaskBean product;

    PooledTaskFactory(AtomicInteger destructions) {
      product = new TaskBean(destructions);
    }

    @Override
    public TaskBean getObject() {
      return product;
    }

    @Override
    public Class<?> getObjectType() {
      return TaskBean.class;
    }

    @Override
    public boolean isSingleton() {
      return true;
    }

    @Override
    public boolean isPrototype() {
      return false;
    }
  }

  static class NonIndependentTaskFactory
      implements org.springframework.beans.factory.SmartFactoryBean<TaskBean> {
    final TaskBean product;

    NonIndependentTaskFactory(AtomicInteger destructions) {
      product = new TaskBean(destructions);
    }

    @Override
    public TaskBean getObject() {
      return product;
    }

    @Override
    public Class<?> getObjectType() {
      return TaskBean.class;
    }

    @Override
    public boolean isSingleton() {
      return false;
    }

    @Override
    public boolean isPrototype() {
      return false;
    }
  }

  @Test
  void factoryBeanProductRetainsInferredAutoCloseableCleanup() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger factoryCreations = new AtomicInteger();
      AutoCloseableTaskFactory factory = new AutoCloseableTaskFactory();
      context.registerBean(
          "autoCloseableProduct",
          AutoCloseableTaskFactory.class,
          () -> {
            factoryCreations.incrementAndGet();
            return factory;
          },
          definition -> definition.setScope("prototype"));
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      AutoCloseableTask product;
      try (var handle = resolver.acquire(AutoCloseableTask.class)) {
        product = (AutoCloseableTask) handle.instance();
        assertThat(factory.products).hasValue(1);
        assertThat(product.closes).hasValue(0);
      }
      assertThat(factory.products).hasValue(1);
      assertThat(factoryCreations).hasValue(1);
      assertThat(product.closes).hasValue(1);
    }
  }

  static class AutoCloseableTask implements Task, AutoCloseable {
    final AtomicInteger closes = new AtomicInteger();

    @Override
    public void run() {}

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }

  static class AutoCloseableTaskFactory
      implements org.springframework.beans.factory.SmartFactoryBean<AutoCloseableTask> {
    final AtomicInteger products = new AtomicInteger();

    @Override
    public AutoCloseableTask getObject() {
      products.incrementAndGet();
      return new AutoCloseableTask();
    }

    @Override
    public Class<?> getObjectType() {
      return AutoCloseableTask.class;
    }

    @Override
    public boolean isSingleton() {
      return false;
    }

    @Override
    public boolean isPrototype() {
      return true;
    }
  }

  @Test
  void factoryBeanFallbackInvokesEachProductCallbackOnce() {
    try (var context = new AnnotationConfigApplicationContext()) {
      CallbackTaskFactory factory = new CallbackTaskFactory();
      context.registerBean(
          "callbackProduct",
          CallbackTaskFactory.class,
          () -> factory,
          definition -> definition.setDestroyMethodName("shutdownFactory"));
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      CallbackTask product;
      try (var handle = resolver.acquire(CallbackTask.class)) {
        product = (CallbackTask) handle.instance();
      }
      assertThat(product.preDestroys).hasValue(1);
      assertThat(product.disposals).hasValue(1);
      assertThat(factory.shutdowns).hasValue(0);
    }
  }

  @Test
  void factoryBeanDoesNotReplayCallbacksWhenAPostProcessorFailsValidation() {
    try (var context = new AnnotationConfigApplicationContext()) {
      CallbackTaskFactory factory = new CallbackTaskFactory();
      context.registerBean(
          "postProcessorProduct",
          CallbackTaskFactory.class,
          () -> factory,
          definition -> definition.setScope("prototype"));
      CountingDestructionPostProcessor first = new CountingDestructionPostProcessor();
      FailingDestructionPostProcessor second = new FailingDestructionPostProcessor();
      context.getBeanFactory().addBeanPostProcessor(first);
      context.getBeanFactory().addBeanPostProcessor(second);
      context.refresh();

      var resolver = new SpringBeanResolver(context.getBeanFactory());
      var handle = resolver.acquire(CallbackTask.class);
      assertThatThrownBy(handle::close).isInstanceOf(BeanDefinitionValidationException.class);
      assertThat(first.calls).hasValue(1);
      assertThat(second.calls).hasValue(1);
    }
  }

  @Test
  void factoryBeanDoesNotReplayCallbacksWhenANestedDestroyFailsValidation() {
    try (var context = new AnnotationConfigApplicationContext()) {
      CallbackTaskFactory outer = new CallbackTaskFactory();
      CallbackTaskFactory nested = new CallbackTaskFactory();
      context.registerBean("outerProduct", CallbackTaskFactory.class, () -> outer);
      context.registerBean(
          "nestedProduct",
          CallbackTaskFactory.class,
          () -> nested,
          definition -> definition.setDestroyMethodName("shutdownFactory"));
      NestedDestroyingPostProcessor postProcessor =
          new NestedDestroyingPostProcessor(context.getBeanFactory());
      context.getBeanFactory().addBeanPostProcessor(postProcessor);
      context.refresh();

      var handle = SpringManagedBeans.acquire(context.getBeanFactory(), "outerProduct");
      assertThatThrownBy(handle::close).isInstanceOf(BeanDefinitionValidationException.class);
      assertThat(postProcessor.calls).hasValue(1);
    }
  }

  @Test
  void factoryBeanMetadataFallbackWorksInsideAnUnrelatedDestructionCallback() {
    try (var context = new AnnotationConfigApplicationContext()) {
      CallbackTaskFactory factory = new CallbackTaskFactory();
      AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> handle = new AtomicReference<>();
      context.registerBean(
          "callbackProduct",
          CallbackTaskFactory.class,
          () -> factory,
          definition -> definition.setDestroyMethodName("shutdownFactory"));
      context.registerBean(
          "unrelatedDestroyer", UnrelatedDestroyer.class, () -> new UnrelatedDestroyer(handle));
      context.refresh();

      var managed = SpringManagedBeans.acquire(context.getBeanFactory(), "callbackProduct");
      CallbackTask product = (CallbackTask) managed.instance();
      handle.set(managed);
      assertThatCode(
              () ->
                  context
                      .getBeanFactory()
                      .destroyBean("unrelatedDestroyer", context.getBean("unrelatedDestroyer")))
          .doesNotThrowAnyException();
      assertThat(product.preDestroys).hasValue(1);
      assertThat(product.disposals).hasValue(1);
    }
  }

  @Test
  void factoryBeanDoesNotRetryOuterCallbacksWhenTheyCloseAnOrdinaryInvalidPrototype() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> ordinary = new AtomicReference<>();
      context.registerBean("outerProduct", OuterTaskFactory.class, OuterTaskFactory::new);
      context.registerBean(
          "ordinaryProduct",
          CallbackTask.class,
          CallbackTask::new,
          definition -> {
            definition.setScope("prototype");
            definition.setDestroyMethodName("shutdownFactory");
          });
      OrdinaryHandleClosingPostProcessor postProcessor =
          new OrdinaryHandleClosingPostProcessor(ordinary);
      context.getBeanFactory().addBeanPostProcessor(postProcessor);
      context.refresh();

      ordinary.set(SpringManagedBeans.acquire(context.getBeanFactory(), "ordinaryProduct"));
      var outer = SpringManagedBeans.acquire(context.getBeanFactory(), "outerProduct");
      assertThatThrownBy(outer::close).isInstanceOf(BeanDefinitionValidationException.class);
      assertThat(postProcessor.calls).hasValue(1);
    }
  }

  @Test
  void factoryBeanFallbackUnwindsBeforeItsProductOnlyCallbacksRun() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> nested = new AtomicReference<>();
      context.registerBean("outerProduct", OuterTaskFactory.class, OuterTaskFactory::new);
      context.registerBean(
          "nestedProduct",
          FallbackTaskFactory.class,
          FallbackTaskFactory::new,
          definition -> definition.setDestroyMethodName("shutdownFactory"));
      context.registerBean(
          "ordinaryProduct",
          CallbackTask.class,
          CallbackTask::new,
          definition -> {
            definition.setScope("prototype");
            definition.setDestroyMethodName("shutdownFactory");
          });
      FactoryHandleClosingPostProcessor outerPostProcessor =
          new FactoryHandleClosingPostProcessor(nested);
      FailingFallbackProductPostProcessor nestedPostProcessor =
          new FailingFallbackProductPostProcessor(context.getBeanFactory());
      context.getBeanFactory().addBeanPostProcessor(outerPostProcessor);
      context.getBeanFactory().addBeanPostProcessor(nestedPostProcessor);
      context.refresh();

      nested.set(SpringManagedBeans.acquire(context.getBeanFactory(), "nestedProduct"));
      var outer = SpringManagedBeans.acquire(context.getBeanFactory(), "outerProduct");
      assertThatThrownBy(outer::close).isInstanceOf(BeanDefinitionValidationException.class);
      assertThat(outerPostProcessor.calls).hasValue(1);
      assertThat(nestedPostProcessor.calls).hasValue(1);
    }
  }

  @Test
  void factoryBeanDoesNotRetryWhenNamedAdapterFiltersPostProcessors() {
    try (var context = new AnnotationConfigApplicationContext()) {
      context.registerBean("outerProduct", CallbackTaskFactory.class, CallbackTaskFactory::new);
      context.registerBean(
          "ordinaryProduct",
          CallbackTask.class,
          CallbackTask::new,
          definition -> {
            definition.setScope("prototype");
            definition.setDestroyMethodName("shutdownFactory");
          });
      FilterDestroyingPostProcessor postProcessor =
          new FilterDestroyingPostProcessor(context.getBeanFactory());
      context.getBeanFactory().addBeanPostProcessor(postProcessor);
      context.refresh();

      var managed = SpringManagedBeans.acquire(context.getBeanFactory(), "outerProduct");
      assertThatThrownBy(managed::close).isInstanceOf(BeanDefinitionValidationException.class);
      assertThat(postProcessor.calls).hasValue(1);
    }
  }

  static class CountingDestructionPostProcessor implements DestructionAwareBeanPostProcessor {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public boolean requiresDestruction(Object bean) {
      return bean instanceof CallbackTask;
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {
      calls.incrementAndGet();
    }
  }

  static class FailingDestructionPostProcessor extends CountingDestructionPostProcessor {
    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {
      super.postProcessBeforeDestruction(bean, beanName);
      throw new BeanDefinitionValidationException(
          "Could not find a destroy method named 'shutdownFactory' on bean with name '"
              + beanName
              + "'");
    }
  }

  static class NestedDestroyingPostProcessor implements DestructionAwareBeanPostProcessor {
    private final ConfigurableListableBeanFactory beanFactory;
    final AtomicInteger calls = new AtomicInteger();

    NestedDestroyingPostProcessor(ConfigurableListableBeanFactory beanFactory) {
      this.beanFactory = beanFactory;
    }

    @Override
    public boolean requiresDestruction(Object bean) {
      return bean instanceof CallbackTask;
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {
      calls.incrementAndGet();
      beanFactory.destroyBean("nestedProduct", new CallbackTask());
    }
  }

  static class UnrelatedDestroyer {
    private final AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> handle;

    UnrelatedDestroyer(AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> handle) {
      this.handle = handle;
    }

    @PreDestroy
    void destroy() {
      handle.get().close();
    }
  }

  static class OrdinaryHandleClosingPostProcessor implements DestructionAwareBeanPostProcessor {
    private final AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> ordinary;
    final AtomicInteger calls = new AtomicInteger();

    OrdinaryHandleClosingPostProcessor(
        AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> ordinary) {
      this.ordinary = ordinary;
    }

    @Override
    public boolean requiresDestruction(Object bean) {
      return bean instanceof OuterTask;
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {
      calls.incrementAndGet();
      ordinary.get().close();
    }
  }

  static class FactoryHandleClosingPostProcessor implements DestructionAwareBeanPostProcessor {
    private final AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> nested;
    final AtomicInteger calls = new AtomicInteger();

    FactoryHandleClosingPostProcessor(
        AtomicReference<run.ratchet.spi.BeanResolver.ManagedBean> nested) {
      this.nested = nested;
    }

    @Override
    public boolean requiresDestruction(Object bean) {
      return bean instanceof OuterTask;
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {
      calls.incrementAndGet();
      nested.get().close();
    }
  }

  static class FailingFallbackProductPostProcessor implements DestructionAwareBeanPostProcessor {
    private final ConfigurableListableBeanFactory beanFactory;
    final AtomicInteger calls = new AtomicInteger();

    FailingFallbackProductPostProcessor(ConfigurableListableBeanFactory beanFactory) {
      this.beanFactory = beanFactory;
    }

    @Override
    public boolean requiresDestruction(Object bean) {
      if (!(bean instanceof FallbackTask)) return false;
      calls.incrementAndGet();
      beanFactory.destroyBean("ordinaryProduct", new CallbackTask());
      return true;
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {}
  }

  static class FilterDestroyingPostProcessor implements DestructionAwareBeanPostProcessor {
    private final ConfigurableListableBeanFactory beanFactory;
    final AtomicInteger calls = new AtomicInteger();

    FilterDestroyingPostProcessor(ConfigurableListableBeanFactory beanFactory) {
      this.beanFactory = beanFactory;
    }

    @Override
    public boolean requiresDestruction(Object bean) {
      if (!(bean instanceof CallbackTask)) return false;
      calls.incrementAndGet();
      beanFactory.destroyBean("ordinaryProduct", new CallbackTask());
      return true;
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) {}
  }

  static class CallbackTask implements Task, org.springframework.beans.factory.DisposableBean {
    final AtomicInteger preDestroys = new AtomicInteger();
    final AtomicInteger disposals = new AtomicInteger();

    @Override
    public void run() {}

    @PreDestroy
    public void preDestroy() {
      preDestroys.incrementAndGet();
    }

    @Override
    public void destroy() {
      disposals.incrementAndGet();
    }
  }

  static class CallbackTaskFactory
      implements org.springframework.beans.factory.SmartFactoryBean<CallbackTask> {
    final AtomicInteger shutdowns = new AtomicInteger();

    @Override
    public CallbackTask getObject() {
      return new CallbackTask();
    }

    @Override
    public Class<?> getObjectType() {
      return CallbackTask.class;
    }

    @Override
    public boolean isSingleton() {
      return false;
    }

    @Override
    public boolean isPrototype() {
      return true;
    }

    public void shutdownFactory() {
      shutdowns.incrementAndGet();
    }
  }

  static class OuterTask extends CallbackTask {}

  static class OuterTaskFactory extends CallbackTaskFactory {
    @Override
    public CallbackTask getObject() {
      return new OuterTask();
    }
  }

  static class FallbackTask extends CallbackTask {}

  static class FallbackTaskFactory extends CallbackTaskFactory {
    @Override
    public CallbackTask getObject() {
      return new FallbackTask();
    }
  }
}
