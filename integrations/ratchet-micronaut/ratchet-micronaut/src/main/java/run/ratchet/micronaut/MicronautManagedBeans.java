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
package run.ratchet.micronaut;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.inject.BeanDefinition;
import java.util.concurrent.atomic.AtomicBoolean;
import run.ratchet.spi.BeanResolver.ManagedBean;

/** Owns destruction only for independent prototype products; scoped beans remain context-owned. */
final class MicronautManagedBeans {
  private MicronautManagedBeans() {}

  static <T> ManagedBean acquire(BeanContext context, BeanDefinition<T> definition) {
    var registration = context.getBeanRegistration(definition);
    T bean = registration.getBean();
    boolean prototype = definition.getScope().filter(Prototype.class::equals).isPresent();
    return new ManagedBean() {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public Object instance() {
        return bean;
      }

      @Override
      public void close() {
        // Destroy the registration so Micronaut also releases its dependent beans.
        // Factory products declare their disposal method with @Bean(preDestroy = "...").
        if (prototype && closed.compareAndSet(false, true)) context.destroyBean(registration);
      }
    };
  }
}
