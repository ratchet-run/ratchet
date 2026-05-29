package run.ratchet.api.internal;

import java.util.Optional;
import run.ratchet.spi.RatchetConfigSource;

/**
 * Last-resort configuration source: environment variables first, then system properties.
 *
 * <p><b>Framework-internal:</b> applications must not depend on this class. Used by {@link
 * run.ratchet.api.RatchetOptionsFactory#fromEnvironment} to anchor the ambient configuration chain.
 *
 * @since 0.1.0
 */
public final class EnvironmentRatchetConfigSource implements RatchetConfigSource {

  @Override
  public Optional<String> get(String propertyName, String environmentVariable) {
    if (environmentVariable != null && !environmentVariable.isBlank()) {
      String env = System.getenv(environmentVariable);
      if (env != null && !env.isBlank()) {
        return Optional.of(env);
      }
    }

    if (propertyName != null && !propertyName.isBlank()) {
      String property = System.getProperty(propertyName);
      if (property != null && !property.isBlank()) {
        return Optional.of(property);
      }
    }

    return Optional.empty();
  }
}
