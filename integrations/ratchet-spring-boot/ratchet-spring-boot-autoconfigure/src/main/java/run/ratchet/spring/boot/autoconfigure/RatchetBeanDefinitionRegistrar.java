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
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import run.ratchet.ri.runtime.RatchetComponentDescriptor;
import run.ratchet.ri.runtime.RatchetRuntimeComponentCatalog;
import run.ratchet.spi.AfterCommitRegistrar;

/**
 * Registers catalogued Ratchet components for lazy, container-managed construction.
 *
 * <p>Each definition pins the catalog's selected constructor so constructor selection cannot drift
 * independently from the portable runtime metadata.
 */
public final class RatchetBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {

  @Override
  public void registerBeanDefinitions(
      AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
    RatchetRuntimeComponentCatalog.components().stream()
        .filter(
            descriptor -> !AfterCommitRegistrar.class.isAssignableFrom(descriptor.componentType()))
        .forEach(descriptor -> registerComponent(descriptor, registry));
  }

  private static void registerComponent(
      RatchetComponentDescriptor descriptor, BeanDefinitionRegistry registry) {
    Constructor<?> constructor = selectedConstructor(descriptor);
    RootBeanDefinition beanDefinition = new RootBeanDefinition(descriptor.componentType());
    beanDefinition.setLazyInit(true);
    beanDefinition.setScope(
        descriptor.singletonScope()
            ? BeanDefinition.SCOPE_SINGLETON
            : BeanDefinition.SCOPE_PROTOTYPE);
    beanDefinition.setAttribute(
        AbstractBeanDefinition.PREFERRED_CONSTRUCTORS_ATTRIBUTE, constructor);
    registry.registerBeanDefinition(descriptor.componentType().getName(), beanDefinition);
  }

  private static Constructor<?> selectedConstructor(RatchetComponentDescriptor descriptor) {
    Class<?>[] parameterTypes = descriptor.constructorParameterTypes().toArray(Class<?>[]::new);
    try {
      return descriptor.componentType().getConstructor(parameterTypes);
    } catch (NoSuchMethodException exception) {
      throw new BeanDefinitionStoreException(
          "Catalogued constructor is missing for " + descriptor.componentType().getName(),
          exception);
    }
  }
}
