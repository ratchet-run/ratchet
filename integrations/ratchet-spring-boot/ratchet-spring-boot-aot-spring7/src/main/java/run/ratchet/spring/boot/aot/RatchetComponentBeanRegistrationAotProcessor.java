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
package run.ratchet.spring.boot.aot;

import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.STATIC;

import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.aot.generate.GeneratedMethod;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.aot.BeanRegistrationCode;
import org.springframework.beans.factory.aot.BeanRegistrationCodeFragments;
import org.springframework.beans.factory.aot.BeanRegistrationCodeFragmentsDecorator;
import org.springframework.beans.factory.support.InstanceSupplier;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.javapoet.ClassName;
import org.springframework.javapoet.CodeBlock;
import run.ratchet.ri.runtime.RatchetRuntimeComponentCatalog;
import run.ratchet.spring.boot.autoconfigure.RatchetBeanDefinitionRegistrar;

/**
 * Generates AOT-safe instance suppliers for the Ratchet runtime component catalog.
 *
 * <p>The ordinary Spring registration deliberately uses an instance supplier so Ratchet's portable
 * component catalog remains the authority for constructor selection. Spring cannot serialize an
 * arbitrary supplier into generated bean-definition code, so this processor replaces it with a
 * generated {@link InstanceSupplier} that calls the same Spring-local construction path.
 */
public final class RatchetComponentBeanRegistrationAotProcessor
    implements BeanRegistrationAotProcessor {

  private static final Set<Class<?>> RATCHET_COMPONENT_TYPES =
      RatchetRuntimeComponentCatalog.components().stream()
          .map(descriptor -> descriptor.componentType())
          .collect(Collectors.toUnmodifiableSet());

  @Override
  public BeanRegistrationAotContribution processAheadOfTime(RegisteredBean registeredBean) {
    if (registeredBean.getMergedBeanDefinition().getInstanceSupplier() == null
        || !RATCHET_COMPONENT_TYPES.contains(registeredBean.getBeanClass())) {
      return null;
    }
    return new RatchetComponentBeanRegistrationAotContribution(
        registeredBean,
        RatchetBeanDefinitionRegistrar.resolveConstructor(registeredBean.getBeanClass()));
  }

  private static final class RatchetComponentBeanRegistrationAotContribution
      implements BeanRegistrationAotContribution {

    private final RegisteredBean registeredBean;
    private final Constructor<?> constructor;

    private RatchetComponentBeanRegistrationAotContribution(
        RegisteredBean registeredBean, Constructor<?> constructor) {
      this.registeredBean = registeredBean;
      this.constructor = constructor;
    }

    @Override
    public BeanRegistrationCodeFragments customizeBeanRegistrationCodeFragments(
        GenerationContext generationContext, BeanRegistrationCodeFragments codeFragments) {
      return new RatchetComponentCodeFragments(codeFragments, registeredBean);
    }

    @Override
    public void applyTo(
        GenerationContext generationContext, BeanRegistrationCode beanRegistrationCode) {
      generationContext
          .getRuntimeHints()
          .reflection()
          .registerConstructor(constructor, ExecutableMode.INVOKE);
    }
  }

  private static final class RatchetComponentCodeFragments
      extends BeanRegistrationCodeFragmentsDecorator {

    private final RegisteredBean registeredBean;

    private RatchetComponentCodeFragments(
        BeanRegistrationCodeFragments delegate, RegisteredBean registeredBean) {
      super(delegate);
      this.registeredBean = registeredBean;
    }

    @Override
    public ClassName getTarget(RegisteredBean ignored) {
      return ClassName.get(registeredBean.getBeanClass());
    }

    @Override
    public CodeBlock generateInstanceSupplierCode(
        GenerationContext generationContext,
        BeanRegistrationCode beanRegistrationCode,
        boolean allowDirectSupplierShortcut) {
      Class<?> beanType = registeredBean.getBeanClass();
      GeneratedMethod getInstance =
          beanRegistrationCode
              .getMethods()
              .add(
                  "getInstance",
                  method ->
                      method
                          .addJavadoc("Get the catalogued Ratchet component instance.\n")
                          .addModifiers(PRIVATE, STATIC)
                          .returns(beanType)
                          .addParameter(RegisteredBean.class, "registeredBean")
                          .addStatement(
                              "return $T.instantiateForAot($T.class, registeredBean.getBeanFactory())",
                              RatchetBeanDefinitionRegistrar.class,
                              beanType));
      return CodeBlock.of(
          "$T.of($T::$L)",
          InstanceSupplier.class,
          beanRegistrationCode.getClassName(),
          getInstance.getName());
    }
  }
}
