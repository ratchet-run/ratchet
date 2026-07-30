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
   * A container-managed bean resolution whose lifecycle ends when the handle is closed.
   *
   * <p>{@link #close()} releases the resolved instance when the container created it for this
   * resolution, such as a CDI {@code @Dependent} bean or a Spring prototype bean. Closing a handle
   * for a shared-scope bean is a no-op. Handles are single-use and are not thread-safe.
   *
   * @param <T> resolved bean type
   */
  interface ManagedBeanHandle<T> extends AutoCloseable {

    /** Returns the resolved bean instance. */
    T get();

    /** Releases this resolution without throwing a checked exception. */
    @Override
    void close();
  }

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
   * Resolves a bean instance together with its container-managed lifecycle.
   *
   * <p>The default preserves compatibility for existing resolvers by delegating to {@link
   * #resolve(Class)} and returning a handle whose {@link ManagedBeanHandle#close()} operation is a
   * no-op.
   *
   * @param type concrete or assignable bean type to resolve; must not be {@code null}
   * @param <T> resolved bean type
   * @return managed handle for the resolved bean; never {@code null}
   */
  default <T> ManagedBeanHandle<T> resolveManaged(Class<T> type) {
    T bean = resolve(type);
    return new ManagedBeanHandle<>() {
      @Override
      public T get() {
        return bean;
      }

      @Override
      public void close() {
        // Existing resolvers have no per-resolution lifecycle to release.
      }
    };
  }
}
