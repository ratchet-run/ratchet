package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Resolves bean instances by type, abstracting the dependency injection mechanism. The CDI
 * implementation delegates to CDI.current().select(type).get(), while the default RI implementation
 * uses reflection.
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
   * @throws IllegalStateException if no instance can be resolved or if more than one bean is
   *     eligible
   */
  <T> T resolve(Class<T> type);
}
