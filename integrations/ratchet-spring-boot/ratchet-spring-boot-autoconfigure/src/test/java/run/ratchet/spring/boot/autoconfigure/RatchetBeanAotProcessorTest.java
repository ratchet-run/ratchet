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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ContextAnnotationAutowireCandidateResolver;
import org.springframework.context.annotation.Lazy;

class RatchetBeanAotProcessorTest {
  @Test
  void registersLazyConstructorProxyWithoutConstructingEitherBean() {
    var factory = new DefaultListableBeanFactory();
    factory.setAutowireCandidateResolver(new ContextAnnotationAutowireCandidateResolver());
    factory.registerBeanDefinition(
        "ratchet", new RootBeanDefinition(RatchetAutoConfiguration.class));
    factory.registerBeanDefinition("target", new RootBeanDefinition(Target.class));
    factory.registerBeanDefinition("consumer", new RootBeanDefinition(Consumer.class));
    var context = mock(GenerationContext.class);
    var hints = new RuntimeHints();
    when(context.getRuntimeHints()).thenReturn(hints);
    new RatchetBeanAotProcessor()
        .processAheadOfTime(RegisteredBean.of(factory, "consumer"))
        .applyTo(context, null);
    assertThat(
            hints
                .reflection()
                .typeHints()
                .filter(hint -> hint.getType().getName().startsWith(Target.class.getName() + "$$")))
        .singleElement()
        .satisfies(
            hint ->
                assertThat(hint.getMemberCategories())
                    .contains(
                        MemberCategory.INVOKE_DECLARED_METHODS, MemberCategory.DECLARED_FIELDS));
    assertThat(factory.containsSingleton("target")).isFalse();
    assertThat(factory.containsSingleton("consumer")).isFalse();
  }

  @Test
  void disabledConfigurationDoesNotAddProxyHints() {
    var factory = new DefaultListableBeanFactory();
    factory.registerBeanDefinition("target", new RootBeanDefinition(Target.class));
    assertThat(
            new RatchetBeanAotProcessor().processAheadOfTime(RegisteredBean.of(factory, "target")))
        .isNull();
  }

  static class Consumer {
    Consumer(@Lazy Target target) {
      throw new AssertionError("AOT constructed consumer");
    }
  }

  public static class Target {
    public Target() {
      throw new AssertionError("AOT constructed lazy target");
    }

    public void execute() {}
  }
}
