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
package run.ratchet.ri.runtime;

import java.lang.reflect.Method;
import java.util.Set;

/**
 * Discovers application bean classes that declare recurring methods.
 *
 * <p>Implementations must return stable user classes only. Proxy and synthetic implementation
 * classes must never escape through this contract.
 *
 * @apiNote This interface is incubating and intended for container integrations.
 */
public interface RecurringMethodDiscovery {

  /** Returns the stable user classes that may contain recurring methods. */
  Set<Class<?>> recurringBeanClasses();

  /**
   * Returns whether the container can invoke this recurring method on the resolved bean.
   *
   * <p>The default supports containers whose managed instances remain assignable to their user
   * classes. An implementation that returns {@code false} is responsible for logging a
   * container-specific diagnostic explaining how to make the method invocable.
   */
  default boolean isMethodInvocable(Class<?> beanClass, Method method) {
    return true;
  }
}
