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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.ClassUtils;
import run.ratchet.api.Recurring;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;

/** Discovers recurring Spring beans without exposing generated proxy classes to Ratchet. */
public final class SpringRecurringMethodDiscovery implements RecurringMethodDiscovery {

  private static final Log log = LogFactory.getLog(SpringRecurringMethodDiscovery.class);

  private final ConfigurableListableBeanFactory beanFactory;

  public SpringRecurringMethodDiscovery(ConfigurableListableBeanFactory beanFactory) {
    this.beanFactory = Objects.requireNonNull(beanFactory, "beanFactory");
  }

  @Override
  public Set<Class<?>> recurringBeanClasses() {
    Set<Class<?>> recurringBeanClasses = new LinkedHashSet<>();
    Arrays.stream(beanFactory.getBeanDefinitionNames())
        .sorted()
        .forEach(
            beanName -> {
              BeanDefinition definition = beanFactory.getBeanDefinition(beanName);
              if (definition.isAbstract() || !definition.isAutowireCandidate()) {
                return;
              }
              Class<?> userClass = SpringBeanResolver.stableUserClass(beanFactory, beanName);
              if (isStableUserClass(userClass) && hasRecurringMethod(userClass)) {
                recurringBeanClasses.add(userClass);
              }
            });
    return Collections.unmodifiableSet(recurringBeanClasses);
  }

  @Override
  public boolean isMethodInvocable(Class<?> beanClass, Method method) {
    String beanName = SpringBeanResolver.selectBeanName(beanFactory, beanClass);
    boolean prototype = beanFactory.isPrototype(beanName);
    Object bean = beanFactory.getBean(beanName);
    try {
      if (!AopUtils.isJdkDynamicProxy(bean) || isDeclaredByProxyInterface(bean, method)) {
        return true;
      }

      log.warn(
          "@Recurring method "
              + beanClass.getName()
              + "."
              + method.getName()
              + " is not invocable through its JDK proxy because the method signature is absent"
              + " from every implemented interface. Declare the method on a proxied interface or"
              + " set spring.aop.proxy-target-class=true.");
      return false;
    } finally {
      if (prototype) {
        beanFactory.destroyBean(bean);
      }
    }
  }

  private static boolean hasRecurringMethod(Class<?> userClass) {
    Class<?> current = userClass;
    while (current != null && current != Object.class) {
      for (Method method : current.getDeclaredMethods()) {
        if (!method.isSynthetic()
            && !method.isBridge()
            && method.isAnnotationPresent(Recurring.class)) {
          return true;
        }
      }
      current = current.getSuperclass();
    }
    return false;
  }

  private static boolean isStableUserClass(Class<?> userClass) {
    return userClass != null && !userClass.isSynthetic() && !Proxy.isProxyClass(userClass);
  }

  private static boolean isDeclaredByProxyInterface(Object bean, Method method) {
    for (Class<?> implementedInterface : ClassUtils.getAllInterfacesAsSet(bean)) {
      try {
        implementedInterface.getMethod(method.getName(), method.getParameterTypes());
        return true;
      } catch (NoSuchMethodException ignored) {
        // Inspect the next proxied interface.
      }
    }
    return false;
  }
}
