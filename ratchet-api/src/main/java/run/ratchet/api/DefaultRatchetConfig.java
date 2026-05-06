package run.ratchet.api;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import run.ratchet.spi.RatchetConfigSource;

/** Default typed Ratchet configuration facade over an ordered source chain. */
final class DefaultRatchetConfig implements RatchetConfig {

  private final List<RatchetConfigSource> sources;

  DefaultRatchetConfig(List<RatchetConfigSource> sources) {
    this.sources = List.copyOf(Objects.requireNonNull(sources, "sources must not be null"));
  }

  @Override
  public <T> T get(RatchetConfigKey<T> key) {
    return key.parse(raw(key).orElse(null));
  }

  @Override
  public Optional<String> raw(RatchetConfigKey<?> key) {
    return lookup(key.name(), key.environmentVariable());
  }

  private Optional<String> lookup(String propertyName, String environmentVariable) {
    for (RatchetConfigSource source : sources) {
      Optional<String> value = source.get(propertyName, environmentVariable);
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }
}
