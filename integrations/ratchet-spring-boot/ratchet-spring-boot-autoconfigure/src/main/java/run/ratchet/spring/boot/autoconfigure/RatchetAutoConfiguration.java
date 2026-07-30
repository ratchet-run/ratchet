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

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;

@AutoConfiguration
@Import(RatchetBeanDefinitionRegistrar.class)
public class RatchetAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(AfterCommitRegistrar.class)
  AfterCommitRegistrar afterCommitRegistrar() {
    return new SpringAfterCommitRegistrar();
  }

  @Bean
  @ConditionalOnMissingBean(BeanResolver.class)
  BeanResolver beanResolver(ConfigurableListableBeanFactory beanFactory) {
    return new SpringBeanResolver(beanFactory);
  }

  @Bean
  @ConditionalOnMissingBean(RecurringMethodDiscovery.class)
  RecurringMethodDiscovery recurringMethodDiscovery(ConfigurableListableBeanFactory beanFactory) {
    return new SpringRecurringMethodDiscovery(beanFactory);
  }
}
