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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.reflection;

import java.lang.reflect.Constructor;
import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.GeneratedClasses;
import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.beans.factory.support.RootBeanDefinition;
import run.ratchet.ri.core.BatchRecoveryService;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spring.boot.autoconfigure.RatchetBeanDefinitionRegistrar;

class RatchetComponentBeanRegistrationAotProcessorTest {

  @Test
  void registersOnlySelectedConstructorForCataloguedBeanDefinition() throws NoSuchMethodException {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    RootBeanDefinition beanDefinition = new RootBeanDefinition(JobPayloadInvoker.class);
    beanDefinition.setInstanceSupplier(() -> null);
    String beanName = JobPayloadInvoker.class.getName();
    beanFactory.registerBeanDefinition(beanName, beanDefinition);
    RegisteredBean registeredBean = RegisteredBean.of(beanFactory, beanName);

    BeanRegistrationAotContribution contribution =
        new RatchetComponentBeanRegistrationAotProcessor().processAheadOfTime(registeredBean);
    assertNotNull(contribution);

    RuntimeHints hints = new RuntimeHints();
    contribution.applyTo(new HintsGenerationContext(hints, new InMemoryGeneratedFiles()), null);

    Constructor<?> constructor =
        RatchetBeanDefinitionRegistrar.resolveConstructor(JobPayloadInvoker.class);
    assertArrayEquals(
        new Class<?>[] {BeanResolver.class, ClassPolicy.class}, constructor.getParameterTypes());
    assertTrue(reflection().onConstructorInvocation(constructor).test(hints));
    assertFalse(
        reflection()
            .onConstructorInvocation(JobPayloadInvoker.class.getDeclaredConstructor())
            .test(hints));
    assertFalse(
        reflection()
            .onConstructorInvocation(
                RatchetBeanDefinitionRegistrar.resolveConstructor(BatchRecoveryService.class))
            .test(hints));
  }

  private record HintsGenerationContext(RuntimeHints hints, GeneratedFiles files)
      implements GenerationContext {

    @Override
    public GeneratedClasses getGeneratedClasses() {
      throw new UnsupportedOperationException();
    }

    @Override
    public GeneratedFiles getGeneratedFiles() {
      return files;
    }

    @Override
    public RuntimeHints getRuntimeHints() {
      return hints;
    }

    @Override
    public GenerationContext withName(String name) {
      return this;
    }
  }
}
