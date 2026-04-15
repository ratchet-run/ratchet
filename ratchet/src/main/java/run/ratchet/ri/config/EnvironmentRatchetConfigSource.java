package run.ratchet.ri.config;

import run.ratchet.spi.RatchetConfigSource;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

/** Default configuration source: environment variables first, then system properties. */
@ApplicationScoped
public class EnvironmentRatchetConfigSource implements RatchetConfigSource {

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

    if (environmentVariable != null && !environmentVariable.isBlank()) {
      String property = System.getProperty(environmentVariable);
      if (property != null && !property.isBlank()) {
        return Optional.of(property);
      }
    }

    return Optional.empty();
  }
}
