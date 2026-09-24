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

import java.util.Set;
import org.springframework.aop.framework.autoproxy.AutoProxyUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import run.ratchet.store.spi.JobStore;

/**
 * Explicit wiring of the shared Ratchet engine using Spring-managed services and store
 * capabilities.
 */
@AutoConfiguration(
    after = RatchetAutoConfiguration.class,
    afterName = {
      "run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration",
      "run.ratchet.spring.boot.autoconfigure.mongodb.RatchetMongoAutoConfiguration"
    })
@ConditionalOnBean(JobStore.class)
@ConditionalOnProperty(
    prefix = "ratchet",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@Import({
  RatchetPolicyConfiguration.class,
  RatchetSubmissionConfiguration.class,
  RatchetExecutionConfiguration.class,
  RatchetRuntimeConfiguration.class
})
public class RatchetEngineAutoConfiguration {
  private static final Set<String> ENGINE_CONFIGURATIONS =
      Set.of(
          RatchetEngineAutoConfiguration.class.getName(),
          RatchetPolicyConfiguration.class.getName(),
          RatchetSubmissionConfiguration.class.getName(),
          RatchetExecutionConfiguration.class.getName(),
          RatchetRuntimeConfiguration.class.getName());

  @Bean
  static BeanFactoryPostProcessor ratchetPreserveEngineTargetClasses() {
    return beanFactory -> {
      for (String name : beanFactory.getBeanDefinitionNames()) {
        BeanDefinition definition = beanFactory.getBeanDefinition(name);
        String factory = definition.getFactoryBeanName();
        if (factory != null
            && beanFactory.containsBeanDefinition(factory)
            && ENGINE_CONFIGURATIONS.contains(
                beanFactory.getBeanDefinition(factory).getBeanClassName())) {
          definition.setAttribute(AutoProxyUtils.PRESERVE_TARGET_CLASS_ATTRIBUTE, Boolean.TRUE);
        }
      }
    };
  }
}
