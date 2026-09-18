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
package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Resolves bean instances by type, abstracting the dependency injection mechanism. The CDI
 * implementation delegates to CDI.current().select(type).get().
 */
@Incubating
@FunctionalInterface
public interface BeanResolver {
  /**
   * Resolves a bean instance for the requested type.
   *
   * @param type concrete or assignable bean type to resolve; must not be {@code null}
   * @return resolved bean instance; never {@code null}
   * @throws NullPointerException if {@code type} is {@code null}
   * @throws IllegalStateException if no instance can be resolved, if more than one bean is
   *     eligible, or if the implementation cannot safely manage the resolved bean lifecycle
   */
  <T> T resolve(Class<T> type);

  /**
   * Checks whether a type can be resolved. Container implementations should use metadata only. The
   * compatibility fallback acquires and releases an instance.
   */
  default void validateResolvable(Class<?> type) {
    try (ManagedBean handle = acquire(type)) {
      handle.instance();
    }
  }

  /** Acquires a managed invocation target, which may be a proxy exposing its public interfaces. */
  default ManagedBean acquire(Class<?> type) {
    Object bean = resolve(type);
    return new ManagedBean() {
      public Object instance() {
        return bean;
      }

      public void close() {}
    };
  }

  /** An invocation-scoped reference; close releases dependent or prototype instances. */
  interface ManagedBean extends AutoCloseable {
    Object instance();

    @Override
    void close();
  }
}
