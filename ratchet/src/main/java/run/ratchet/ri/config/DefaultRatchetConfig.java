package run.ratchet.ri.config;

import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import run.ratchet.spi.RatchetConfigSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

/** Default typed Ratchet configuration facade. */
@ApplicationScoped
public class DefaultRatchetConfig implements RatchetConfig {

  private final RatchetConfigSource source;

  protected DefaultRatchetConfig() {
    this.source = null;
  }

  @Inject
  public DefaultRatchetConfig(RatchetConfigSource source) {
    this.source = source;
  }

  @Override
  public <T> T get(RatchetConfigKey<T> key) {
    return key.parse(raw(key).orElse(null));
  }

  @Override
  public Optional<String> raw(RatchetConfigKey<?> key) {
    Optional<String> preferred = source.get(key.name(), key.environmentVariable());
    if (preferred.isPresent()) {
      return preferred;
    }
    if (key.hasLegacyName() || key.hasLegacyEnvironmentVariable()) {
      return source.get(key.legacyName(), key.legacyEnvironmentVariable());
    }
    return Optional.empty();
  }
}
