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

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.ClassUtils;
import run.ratchet.spi.BeanResolver;

/** Resolves Spring beans by their stable bean-definition type while preserving managed proxies. */
public final class SpringBeanResolver implements BeanResolver {

  private final ConfigurableListableBeanFactory beanFactory;
  private final ConcurrentHashMap<Class<?>, String> beanNameCache = new ConcurrentHashMap<>();

  public SpringBeanResolver(ConfigurableListableBeanFactory beanFactory) {
    this.beanFactory = Objects.requireNonNull(beanFactory, "beanFactory");
  }

  @Override
  public <T> T resolve(Class<T> type) {
    ResolvedBean resolved = resolveBean(type);
    return uncheckedCast(resolved.instance());
  }

  @Override
  public <T> ManagedBeanHandle<T> resolveManaged(Class<T> type) {
    ResolvedBean resolved = resolveBean(type);
    T instance = uncheckedCast(resolved.instance());
    boolean prototype = isPrototype(resolved.beanName());
    AtomicBoolean closed = new AtomicBoolean();

    return new ManagedBeanHandle<>() {
      @Override
      public T get() {
        return instance;
      }

      @Override
      public void close() {
        if (prototype && closed.compareAndSet(false, true)) {
          beanFactory.destroyBean(instance);
        }
      }
    };
  }

  private <T> ResolvedBean resolveBean(Class<T> type) {
    String beanName = beanNameCache.computeIfAbsent(type, t -> selectBeanName(beanFactory, t));
    return new ResolvedBean(beanName, beanFactory.getBean(beanName));
  }

  private boolean isPrototype(String beanName) {
    BeanDefinition definition = beanFactory.getBeanDefinition(beanName);
    return BeanDefinition.SCOPE_PROTOTYPE.equals(definition.getScope())
        || beanFactory.isPrototype(beanName);
  }

  static String selectBeanName(
      ConfigurableListableBeanFactory beanFactory, Class<?> requestedType) {
    Objects.requireNonNull(beanFactory, "beanFactory");
    Objects.requireNonNull(requestedType, "type");

    List<String> candidates = candidateBeanNames(beanFactory, requestedType);
    if (candidates.isEmpty()) {
      throw new IllegalStateException("No Spring bean found for type: " + requestedType.getName());
    }
    if (candidates.size() == 1) {
      return candidates.get(0);
    }

    List<String> primaryCandidates =
        candidates.stream()
            .filter(name -> beanFactory.getBeanDefinition(name).isPrimary())
            .toList();
    if (primaryCandidates.size() == 1) {
      return primaryCandidates.get(0);
    }

    throw new IllegalStateException(
        "Multiple Spring beans found for type: "
            + requestedType.getName()
            + ". Use @Primary to disambiguate.");
  }

  static List<String> candidateBeanNames(
      ConfigurableListableBeanFactory beanFactory, Class<?> requestedType) {
    List<String> candidates = new ArrayList<>();
    Arrays.stream(beanFactory.getBeanDefinitionNames())
        .sorted()
        .forEach(
            beanName -> {
              BeanDefinition definition = beanFactory.getBeanDefinition(beanName);
              if (definition.isAbstract() || !definition.isAutowireCandidate()) {
                return;
              }
              Class<?> userClass = stableUserClass(beanFactory, beanName);
              if (userClass != null && requestedType.isAssignableFrom(userClass)) {
                candidates.add(beanName);
              }
            });
    return List.copyOf(candidates);
  }

  static Class<?> stableUserClass(ConfigurableListableBeanFactory beanFactory, String beanName) {
    Class<?> beanType = beanFactory.getType(beanName, false);
    if (beanType == null) {
      beanType = beanFactory.getType(beanName);
    }

    Class<?> definitionType = beanFactory.getBeanDefinition(beanName).getResolvableType().resolve();
    if (beanType == null) {
      return definitionType == null ? null : ClassUtils.getUserClass(definitionType);
    }

    Class<?> userClass = ClassUtils.getUserClass(beanType);
    if (definitionType == null) {
      return userClass;
    }

    Class<?> definitionUserClass = ClassUtils.getUserClass(definitionType);
    if (Proxy.isProxyClass(beanType)
        || (userClass.isInterface() && userClass.isAssignableFrom(definitionUserClass))) {
      return definitionUserClass;
    }
    return userClass;
  }

  @SuppressWarnings("unchecked")
  private static <T> T uncheckedCast(Object instance) {
    return (T) instance;
  }

  private record ResolvedBean(String beanName, Object instance) {}
}
