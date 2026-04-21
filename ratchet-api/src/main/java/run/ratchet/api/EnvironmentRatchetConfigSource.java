package run.ratchet.api;

import run.ratchet.spi.RatchetConfigSource;
import java.util.Optional;

/** Last-resort configuration source: environment variables first, then system properties. */
final class EnvironmentRatchetConfigSource implements RatchetConfigSource {

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
