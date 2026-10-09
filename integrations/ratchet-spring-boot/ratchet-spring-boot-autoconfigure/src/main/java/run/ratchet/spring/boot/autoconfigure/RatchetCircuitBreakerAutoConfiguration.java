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

import org.springframework.aop.Advisor;
import org.springframework.aop.config.AopConfigUtils;
import org.springframework.aop.support.ComposablePointcut;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.annotation.Role;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfigProvider;

/** Proxy infrastructure is independent of store, engine and transaction-manager availability. */
@AutoConfiguration
@ImportRuntimeHints(RatchetRuntimeHints.class)
public class RatchetCircuitBreakerAutoConfiguration {
  @Bean
  @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
  static Advisor ratchetCircuitBreakerAdvisor(
      ObjectProvider<CircuitBreakerRegistry> registries,
      ObjectProvider<CircuitBreakerConfigProvider> configurations) {
    var pointcut =
        new ComposablePointcut(new AnnotationMatchingPointcut(CircuitBreakerProtected.class, true))
            .union(new AnnotationMatchingPointcut(null, CircuitBreakerProtected.class));
    var advisor =
        new DefaultPointcutAdvisor(
            pointcut, new CircuitBreakerProtectedInterceptor(registries, configurations));
    // Spring's transaction advisor defaults to LOWEST_PRECEDENCE on both supported Boot lanes.
    // Reject an OPEN circuit before transaction advice borrows a connection.
    advisor.setOrder(Ordered.HIGHEST_PRECEDENCE + 100);
    return advisor;
  }

  @Bean
  @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
  static BeanFactoryPostProcessor ratchetCircuitBreakerAutoProxyCreator(Environment environment) {
    return beanFactory -> {
      if (beanFactory instanceof BeanDefinitionRegistry registry) {
        // Reuse/escalate Spring's single creator, including when spring.aop.auto=false or no
        // transaction manager exists. Never replace Boot's AspectJ-capable creator.
        AopConfigUtils.registerAutoProxyCreatorIfNecessary(registry);
        if (environment.getProperty("spring.aop.proxy-target-class", Boolean.class, true)) {
          AopConfigUtils.forceAutoProxyCreatorToUseClassProxying(registry);
        }
      }
    };
  }
}
