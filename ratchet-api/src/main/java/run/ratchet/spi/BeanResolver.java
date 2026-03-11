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
  <T> T resolve(Class<T> type);
}
