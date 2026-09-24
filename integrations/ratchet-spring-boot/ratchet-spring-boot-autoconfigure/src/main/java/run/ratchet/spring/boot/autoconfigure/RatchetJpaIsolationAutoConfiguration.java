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

import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.context.annotation.Bean;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetPersistenceProvider;

/**
 * Keeps application persistence units independent when Ratchet does not use its SQL integration.
 */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
      "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration",
      "run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration",
      "run.ratchet.spring.boot.autoconfigure.jpa.RatchetDisabledJpaAutoConfiguration"
    })
@ConditionalOnClass({EntityManagerFactory.class, LocalContainerEntityManagerFactoryBean.class})
@ConditionalOnMissingClass("run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration")
@ConditionalOnMissingBean(name = "ratchetDisabledJpaMappings")
public class RatchetJpaIsolationAutoConfiguration {
  @Bean
  static BeanPostProcessor ratchetJpaIsolationMappings() {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessBeforeInitialization(Object bean, String beanName) {
        if (bean instanceof LocalContainerEntityManagerFactoryBean factory) {
          RatchetPersistenceProvider.install(
              factory,
              (provider, unit) -> RatchetJpaMappings.withoutImplicitRatchetMapping(unit),
              false);
        }
        return bean;
      }
    };
  }
}
