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
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import run.ratchet.spi.BeanResolver.ManagedBean;

/** Acquires a named Spring bean with ownership of prototype destruction only. */
final class SpringManagedBeans {
  private SpringManagedBeans() {}

  static ManagedBean acquire(ConfigurableListableBeanFactory beanFactory, String name) {
    boolean prototype = beanFactory.isPrototype(name);
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

  static void destroyPrototype(ConfigurableListableBeanFactory factory, String name, Object bean) {
    // Spring does not register destruction callbacks for prototypes. Its destruction processors
    // need the original instance to find @PreDestroy methods absent from a JDK proxy's interfaces.
    // Invocation always uses the proxy; only this lifecycle callback releases its owned target.
    Object disposable = bean;
    Object target;
    while ((target = AopProxyUtils.getSingletonTarget(disposable)) != null) disposable = target;
    factory.destroyBean(name, disposable);
  }
}
