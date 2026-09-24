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
import org.springframework.beans.factory.support.AbstractBeanDefinition;
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
    if (SpringManagedBeans.ownsPrototype(beanFactory, name))
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
    var autowireCandidates =
        candidates.stream()
            .filter(
                name ->
                    !beanFactory.containsBeanDefinition(name)
                        || beanFactory.getBeanDefinition(name).isAutowireCandidate())
            .toList();
    return selectPrimaryOrUniqueBeanName(
        beanFactory, type, autowireCandidates.isEmpty() ? candidates : autowireCandidates);
  }

  /**
   * Selects one primary candidate, a sole candidate, or one default candidate without instantiating
   * candidates.
   */
  public static String selectPrimaryOrUniqueBeanName(
      ConfigurableListableBeanFactory beanFactory, Class<?> type, Collection<String> candidates) {
    var primary =
        candidates.stream()
            .filter(
                name ->
                    beanFactory.containsBeanDefinition(name)
                        && beanFactory.getBeanDefinition(name).isPrimary())
            .toList();
    if (primary.size() == 1) return primary.get(0);
    if (primary.size() > 1) throw ambiguousCandidates(type, candidates);
    var nonFallbackCandidates =
        candidates.stream().filter(name -> !isFallbackCandidate(beanFactory, name)).toList();
    if (nonFallbackCandidates.size() == 1) return nonFallbackCandidates.get(0);
    if (candidates.size() == 1) return candidates.iterator().next();
    var defaultCandidates =
        candidates.stream().filter(name -> isDefaultCandidate(beanFactory, name)).toList();
    if (defaultCandidates.size() == 1) return defaultCandidates.get(0);
    throw ambiguousCandidates(type, candidates);
  }

  private static IllegalStateException ambiguousCandidates(
      Class<?> type, Collection<String> candidates) {
    return new IllegalStateException(
        "Ratchet requires one bean for "
            + type.getName()
            + "; found "
            + candidates
            + ". Declare a single bean or mark the intended bean @Primary.");
  }

  private static boolean isDefaultCandidate(
      ConfigurableListableBeanFactory beanFactory, String name) {
    return !beanFactory.containsBeanDefinition(name)
        || !(beanFactory.getBeanDefinition(name) instanceof AbstractBeanDefinition definition)
        || definition.isDefaultCandidate();
  }

  private static boolean isFallbackCandidate(
      ConfigurableListableBeanFactory beanFactory, String name) {
    return beanFactory.containsBeanDefinition(name)
        && beanFactory.getBeanDefinition(name).isFallback();
  }
}
