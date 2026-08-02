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

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.Supplier;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.AnnotationMetadata;
import run.ratchet.ri.runtime.RatchetComponentDescriptor;
import run.ratchet.ri.runtime.RatchetRuntimeComponentCatalog;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.StartupCoordinator;

/**
 * Registers catalogued Ratchet components for lazy, container-managed construction.
 *
 * <p>Each definition uses an instance supplier that invokes the catalog's selected constructor.
 * Spring still applies its normal bean post-processors to the supplied instance, but constructor
 * selection cannot drift independently from the portable runtime metadata.
 */
public final class RatchetBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {

  @Override
  public void registerBeanDefinitions(
      AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
    RatchetRuntimeComponentCatalog.components().stream()
        .filter(descriptor -> !isSpringProvidedAdapter(descriptor.componentType()))
        .forEach(descriptor -> registerComponent(descriptor, registry));
  }

  private static boolean isSpringProvidedAdapter(Class<?> componentType) {
    return AfterCommitRegistrar.class.isAssignableFrom(componentType)
        || BeanResolver.class.isAssignableFrom(componentType);
  }

  static void registerComponent(
      RatchetComponentDescriptor descriptor, BeanDefinitionRegistry registry) {
    Constructor<?> constructor = selectedConstructor(descriptor);
    BeanFactory beanFactory = beanFactory(registry);
    RootBeanDefinition beanDefinition = new RootBeanDefinition(descriptor.componentType());
    beanDefinition.setLazyInit(true);
    beanDefinition.setScope(
        descriptor.singletonScope()
            ? BeanDefinition.SCOPE_SINGLETON
            : BeanDefinition.SCOPE_PROTOTYPE);
    beanDefinition.setFallback(isReplaceableDefault(descriptor.componentType()));
    beanDefinition.setInstanceSupplier(() -> instantiate(constructor, beanFactory));
    registry.registerBeanDefinition(descriptor.componentType().getName(), beanDefinition);
  }

  private static boolean isReplaceableDefault(Class<?> componentType) {
    return StartupCoordinator.class.isAssignableFrom(componentType)
        || JobLoggerFactory.class.isAssignableFrom(componentType)
        || NodeIdentityProvider.class.isAssignableFrom(componentType)
        || ResultPersistenceStrategy.class.isAssignableFrom(componentType);
  }

  private static Constructor<?> selectedConstructor(RatchetComponentDescriptor descriptor) {
    Class<?>[] parameterTypes = descriptor.constructorParameterTypes().toArray(Class<?>[]::new);
    try {
      Constructor<?> constructor =
          descriptor.componentType().getDeclaredConstructor(parameterTypes);
      constructor.setAccessible(true);
      return constructor;
    } catch (NoSuchMethodException | RuntimeException exception) {
      throw new BeanDefinitionStoreException(
          "Catalogued constructor is missing for " + descriptor.componentType().getName(),
          exception);
    }
  }

  private static BeanFactory beanFactory(BeanDefinitionRegistry registry) {
    if (registry instanceof BeanFactory factory) {
      return factory;
    }
    throw new BeanDefinitionStoreException(
        "Ratchet component registration requires a BeanDefinitionRegistry that is also a BeanFactory");
  }

  private static Object instantiate(Constructor<?> constructor, BeanFactory beanFactory) {
    Object[] arguments = new Object[constructor.getParameterCount()];
    for (int index = 0; index < arguments.length; index++) {
      arguments[index] = resolveArgument(constructor, index, beanFactory);
    }
    return BeanUtils.instantiateClass(constructor, arguments);
  }

  /**
   * Resolves the catalogued constructor used to instantiate a Ratchet component.
   *
   * @param componentType catalogued Ratchet component type
   * @return the catalogued constructor
   */
  public static Constructor<?> resolveConstructor(Class<?> componentType) {
    RatchetComponentDescriptor descriptor =
        RatchetRuntimeComponentCatalog.components().stream()
            .filter(candidate -> candidate.componentType().equals(componentType))
            .findFirst()
            .orElseThrow(
                () ->
                    new BeanDefinitionStoreException(
                        "No catalogued Ratchet component for " + componentType.getName()));
    return selectedConstructor(descriptor);
  }

  /**
   * Instantiates a catalogued component from generated Spring AOT bean-registration code.
   *
   * <p>The generated registration retains the portable runtime catalog's constructor choice and
   * argument resolution rather than asking Spring to infer a different constructor.
   *
   * @param componentType catalogued Ratchet component type
   * @param beanFactory Spring bean factory used to resolve constructor arguments
   * @param <T> component type
   * @return the constructed, not-yet-post-processed component
   */
  public static <T> T instantiateForAot(Class<T> componentType, BeanFactory beanFactory) {
    return componentType.cast(instantiate(resolveConstructor(componentType), beanFactory));
  }

  private static Object resolveArgument(
      Constructor<?> constructor, int parameterIndex, BeanFactory beanFactory) {
    ResolvableType parameterType =
        ResolvableType.forConstructorParameter(constructor, parameterIndex);
    Class<?> rawType = parameterType.resolve();
    if (List.class.equals(rawType)) {
      return beanFactory.getBeanProvider(parameterType.getGeneric(0)).orderedStream().toList();
    }
    if (Supplier.class.equals(rawType)) {
      ObjectProvider<?> supplierProvider = beanFactory.getBeanProvider(parameterType);
      Object supplier = supplierProvider.getIfAvailable();
      if (supplier != null) {
        return supplier;
      }
      ObjectProvider<?> suppliedValueProvider =
          beanFactory.getBeanProvider(parameterType.getGeneric(0));
      return (Supplier<Object>) suppliedValueProvider::getObject;
    }
    return beanFactory.getBeanProvider(parameterType).getIfAvailable();
  }
}
