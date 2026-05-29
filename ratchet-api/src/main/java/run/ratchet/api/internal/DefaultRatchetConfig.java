package run.ratchet.api.internal;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import run.ratchet.spi.RatchetConfigSource;

/**
 * Default typed Ratchet configuration facade over an ordered source chain.
 *
 * <p><b>Framework-internal:</b> applications must not depend on this class. It is exposed only so
 * that {@link run.ratchet.api.RatchetOptionsFactory} can wire MicroProfile Config / environment
 * sources together; the public surface for typed configuration is {@link
 * run.ratchet.spi.RatchetConfig}.
 *
 * @since 0.1.0
 */
public final class DefaultRatchetConfig implements RatchetConfig {

  private static final Logger LOG = Logger.getLogger(DefaultRatchetConfig.class.getName());

  private final List<RatchetConfigSource> sources;

  public DefaultRatchetConfig(List<RatchetConfigSource> sources) {
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
          e,
          () ->
              "Ratchet config source "
                  + source.getClass().getName()
                  + " failed for key '"
                  + propertyName
                  + "' (env '"
                  + environmentVariable
                  + "'); trying the next source");
      return Optional.empty();
    }
  }
}
