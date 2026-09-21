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

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.support.ClassHintUtils;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.config.DependencyDescriptor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.core.MethodParameter;

/** Registers actual Spring proxy types without obtaining a bean or resolving a lazy target. */
public final class RatchetBeanAotProcessor implements BeanRegistrationAotProcessor {
  @Override
  public BeanRegistrationAotContribution processAheadOfTime(RegisteredBean bean) {
    if (!(bean.getBeanFactory() instanceof DefaultListableBeanFactory factory)
        || factory.getBeanNamesForType(RatchetAutoConfiguration.class, true, false).length == 0) {
      return null;
    }
    return (generation, registration) -> {
      var hints = generation.getRuntimeHints();
      Class<?> type = factory.getType(bean.getBeanName(), false);
      if (type != null && !type.getName().startsWith("java.")) {
        hints.reflection().registerType(type, MemberCategory.INVOKE_PUBLIC_METHODS);
        ClassHintUtils.registerProxyIfNecessary(type, hints);
      }
      if (bean.getMergedBeanDefinition().getInstanceSupplier() != null) return;
      var executable = bean.resolveInstantiationDescriptor().executable();
      for (int index = 0; index < executable.getParameterCount(); index++) {
        var parameter = MethodParameter.forExecutable(executable, index);
        var dependency = new DependencyDescriptor(parameter, true);
        Class<?> proxy =
            factory
                .getAutowireCandidateResolver()
                .getLazyResolutionProxyClass(dependency, bean.getBeanName());
        if (proxy != null) ClassHintUtils.registerProxyIfNecessary(proxy, hints);
      }
    };
  }
}
