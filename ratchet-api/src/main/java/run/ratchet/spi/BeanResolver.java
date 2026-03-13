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
   * Resolves an instance of the specified type, abstracting the mechanism for dependency injection.
   *
   * @param <T> the type of the instance to resolve
   * @param type the class of the type to resolve
   * @return an instance of the specified type
   * @throws IllegalStateException if the instance cannot be resolved
   */
  <T> T resolve(Class<T> type);
}
