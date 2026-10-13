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
import java.util.Objects;
import run.ratchet.spi.BeanResolver;

/** Resolves the primary or uniquely qualified bean without unwrapping its Micronaut advice. */
public final class MicronautBeanResolver implements BeanResolver {
  private final BeanContext context;

  public MicronautBeanResolver(BeanContext context) {
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public <T> T resolve(Class<T> type) {
    BeanDefinition<T> definition = context.getBeanDefinition(type);
    if (definition.getScope().filter(Prototype.class::equals).isPresent())
      throw new IllegalStateException("Use acquire() for prototype job bean " + type.getName());
    return context.getBean(definition);
  }

  @Override
  public ManagedBean acquire(Class<?> type) {
    return MicronautManagedBeans.acquire(context, context.getBeanDefinition(type));
  }

  @Override
  public void validateResolvable(Class<?> type) {
    context.getBeanDefinition(type);
  }
}
