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
package run.ratchet.spring.boot.autoconfigure.jpa;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetPersistenceProvider;

/** Filters only the Ratchet-owned implicit mapping, without altering application mappings. */
final class RatchetDisabledJpaMappings implements BeanPostProcessor {
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
}
