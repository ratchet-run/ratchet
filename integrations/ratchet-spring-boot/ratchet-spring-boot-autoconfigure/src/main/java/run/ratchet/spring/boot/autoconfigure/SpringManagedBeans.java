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

import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import run.ratchet.spi.BeanResolver.ManagedBean;

/** Acquires a named Spring bean with ownership of prototype destruction only. */
final class SpringManagedBeans {
  private SpringManagedBeans() {}

  static ManagedBean acquire(ConfigurableListableBeanFactory beanFactory, String name) {
    boolean prototype = ownsPrototype(beanFactory, name);
    Object bean = beanFactory.getBean(name);
    return new ManagedBean() {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public Object instance() {
        return bean;
      }

      @Override
      public void close() {
        if (prototype && closed.compareAndSet(false, true))
          destroyPrototype(beanFactory, name, bean);
      }
    };
  }

  /** Returns whether Ratchet owns release of this independent prototype product. */
  static boolean ownsPrototype(ConfigurableListableBeanFactory factory, String name) {
    String beanName = BeanFactoryUtils.transformedBeanName(name);
    Object singleton = factory.getSingleton(beanName);
    if (!factory.containsBeanDefinition(beanName) && singleton != null) {
      if (BeanFactoryUtils.isFactoryDereference(name) || !(singleton instanceof FactoryBean<?>))
        return false;
      if (singleton instanceof SmartFactoryBean<?> smartFactoryBean)
        return smartFactoryBean.isPrototype();
      return !((FactoryBean<?>) singleton).isSingleton();
    }
    boolean prototype = factory.isPrototype(name);
    if (!prototype || BeanFactoryUtils.isFactoryDereference(name) || !factory.isFactoryBean(name))
      return prototype;
    Object factoryBean = factory.getSingleton(beanName);
    return !(factoryBean instanceof SmartFactoryBean<?> smartFactoryBean)
        || smartFactoryBean.isPrototype();
  }

  static void destroyPrototype(ConfigurableListableBeanFactory factory, String name, Object bean) {
    // Spring does not register destruction callbacks for prototypes. Its destruction processors
    // need the original instance to find @PreDestroy methods absent from a JDK proxy's interfaces.
    // Invocation always uses the proxy; only this lifecycle callback releases its owned target.
    Object disposable = bean;
    Object target;
    while ((target = AopProxyUtils.getSingletonTarget(disposable)) != null) disposable = target;
    String beanName = BeanFactoryUtils.transformedBeanName(name);
    if (!factory.containsBeanDefinition(beanName)) {
      factory.destroyBean(disposable);
      return;
    }
    if (!BeanFactoryUtils.isFactoryDereference(name) && factory.isFactoryBean(name)) {
      if (!destroyFactoryBeanProductWithNamedMetadata(factory, beanName, disposable))
        factory.destroyBean(disposable);
    } else factory.destroyBean(beanName, disposable);
  }

  private static boolean destroyFactoryBeanProductWithNamedMetadata(
      ConfigurableListableBeanFactory factory, String beanName, Object disposable) {
    try {
      factory.destroyBean(beanName, disposable);
      return true;
    } catch (BeanDefinitionValidationException failure) {
      if (!namedFactoryProductValidation(failure)) throw failure;
      // Spring rejected factory metadata before callbacks. It cannot apply to the product, so the
      // caller may use the product-only adapter after this named-destruction boundary unwinds.
      return false;
    }
  }

  private static boolean namedFactoryProductValidation(BeanDefinitionValidationException failure) {
    // Spring reports metadata validation and post-processor failures with the same type. The
    // construction origin plus no intervening adapter destruction before our named-call boundary
    // proves the named route has not invoked a callback; an unknown trace is rethrown.
    StackTraceElement[] trace = failure.getStackTrace();
    if (trace.length == 0 || !namedAdapterConstruction(trace[0])) return false;
    for (StackTraceElement frame : trace) {
      if (frame.getClassName().equals(SpringManagedBeans.class.getName())
          && frame.getMethodName().equals("destroyFactoryBeanProductWithNamedMetadata"))
        return true;
      if (frame
              .getClassName()
              .equals("org.springframework.beans.factory.support.DisposableBeanAdapter")
          && (frame.getMethodName().equals("destroy")
              || frame.getMethodName().equals("filterPostProcessors"))) return false;
    }
    return false;
  }

  private static boolean namedAdapterConstruction(StackTraceElement frame) {
    return frame
            .getClassName()
            .equals("org.springframework.beans.factory.support.DisposableBeanAdapter")
        && (frame.getMethodName().equals("<init>")
            || frame.getMethodName().equals("determineDestroyMethod"));
  }
}
