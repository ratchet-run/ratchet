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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.aop.framework.autoproxy.AutoProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import run.ratchet.spi.BeanResolver;

/** Resolves application beans while keeping Spring advice on the invocation target. */
public final class SpringBeanResolver implements BeanResolver {
  private final ConfigurableListableBeanFactory beanFactory;
  private final ConcurrentMap<Class<?>, String> beanNames = new ConcurrentHashMap<>();

  public SpringBeanResolver(ConfigurableListableBeanFactory beanFactory) {
    this.beanFactory = beanFactory;
  }

  @Override
  public <T> T resolve(Class<T> type) {
    String name = beanName(type);
    if (beanFactory.isPrototype(name))
      throw new IllegalStateException("Use acquire() for prototype job bean " + name);
    Object bean = beanFactory.getBean(name);
    if (!type.isInstance(bean))
      throw new IllegalStateException(
          "Job bean " + name + " uses an interface proxy; use acquire() to retain its advice");
    return type.cast(bean);
  }

  @Override
  public ManagedBean acquire(Class<?> type) {
    return SpringManagedBeans.acquire(beanFactory, beanName(type));
  }

  @Override
  public void validateResolvable(Class<?> type) {
    beanName(type);
  }

  static Class<?> targetType(ConfigurableListableBeanFactory beanFactory, String name) {
    Object singleton = beanFactory.getSingleton(name);
    if (singleton != null && !(singleton instanceof FactoryBean<?>))
      return AopUtils.getTargetClass(singleton);
    if (beanFactory.containsBeanDefinition(name)) {
      Object target =
          beanFactory
              .getMergedBeanDefinition(name)
              .getAttribute(AutoProxyUtils.ORIGINAL_TARGET_CLASS_ATTRIBUTE);
      if (target instanceof Class<?> targetClass) return targetClass;
    }
    return beanFactory.getType(name, false);
  }

  private String beanName(Class<?> type) {
    return beanNames.computeIfAbsent(Objects.requireNonNull(type, "type"), this::findBeanName);
  }

  private String findBeanName(Class<?> type) {
    return selectBeanName(beanFactory, type);
  }

  static String selectBeanName(ConfigurableListableBeanFactory beanFactory, Class<?> type) {
    Set<String> candidates = new LinkedHashSet<>();
    for (String name : beanFactory.getBeanNamesForType(type, true, false)) {
      if (!name.startsWith("scopedTarget.")) candidates.add(name);
    }
    for (String name : beanFactory.getBeanDefinitionNames()) {
      if (name.startsWith("scopedTarget.")) continue;
      Class<?> target = targetType(beanFactory, name);
      if (target != null && type.isAssignableFrom(target)) candidates.add(name);
    }
    return selectPrimaryOrUniqueBeanName(beanFactory, type, candidates);
  }

  /** Selects a unique candidate or the single primary bean without instantiating candidates. */
  public static String selectPrimaryOrUniqueBeanName(
      ConfigurableListableBeanFactory beanFactory, Class<?> type, Collection<String> candidates) {
    if (candidates.size() == 1) return candidates.iterator().next();
    var primary =
        candidates.stream()
            .filter(
                name ->
                    beanFactory.containsBeanDefinition(name)
                        && beanFactory.getBeanDefinition(name).isPrimary())
            .toList();
    if (primary.size() == 1) return primary.get(0);
    throw new IllegalStateException(
        "Ratchet requires one bean for "
            + type.getName()
            + "; found "
            + candidates
            + ". Declare a single bean or mark the intended bean @Primary.");
  }
}
