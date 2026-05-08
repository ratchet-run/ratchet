package run.ratchet.api;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import run.ratchet.spi.RatchetConfigSource;

/** Default typed Ratchet configuration facade over an ordered source chain. */
final class DefaultRatchetConfig implements RatchetConfig {

  private static final Logger LOG = Logger.getLogger(DefaultRatchetConfig.class.getName());

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
      Optional<String> value = read(source, propertyName, environmentVariable);
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }

  private Optional<String> read(
      RatchetConfigSource source, String propertyName, String environmentVariable) {
    try {
      return Objects.requireNonNullElse(
          source.get(propertyName, environmentVariable), Optional.empty());
    } catch (RuntimeException e) {
      LOG.log(
          Level.WARNING,
          () ->
              "Ratchet config source "
                  + source.getClass().getName()
                  + " failed for key '"
                  + propertyName
                  + "' (env '"
                  + environmentVariable
                  + "'): "
                  + e
                  + "; trying the next source");
      return Optional.empty();
    }
  }
}
