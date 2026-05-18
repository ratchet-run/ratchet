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
}
