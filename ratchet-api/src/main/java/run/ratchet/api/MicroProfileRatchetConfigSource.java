package run.ratchet.api;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import run.ratchet.spi.RatchetConfigSource;

/** Optional MicroProfile Config adapter loaded reflectively when MP Config is present. */
final class MicroProfileRatchetConfigSource implements RatchetConfigSource {

  private final Object config;
  private final Method getOptionalValue;

  private MicroProfileRatchetConfigSource(Object config, Method getOptionalValue) {
    this.config = config;
    this.getOptionalValue = getOptionalValue;
  }

  static Optional<RatchetConfigSource> create() {
    try {
      Class<?> provider = Class.forName("org.eclipse.microprofile.config.ConfigProvider");
      Object config = provider.getMethod("getConfig").invoke(null);
      Method getOptionalValue =
          config.getClass().getMethod("getOptionalValue", String.class, Class.class);
      return Optional.of(new MicroProfileRatchetConfigSource(config, getOptionalValue));
    } catch (ClassNotFoundException e) {
      return Optional.empty();
    } catch (IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | RuntimeException e) {
      return Optional.empty();
    }
  }

  @Override
  public Optional<String> get(String propertyName, String environmentVariable) {
    Optional<String> property = getOptional(propertyName);
    if (property.isPresent()) {
      return property;
    }
    return getOptional(environmentVariable);
  }

  private Optional<String> getOptional(String name) {
    if (name == null || name.isBlank()) {
      return Optional.empty();
    }
    try {
      Optional<?> value = (Optional<?>) getOptionalValue.invoke(config, name, String.class);
      return value.map(Object::toString).filter(raw -> !raw.isBlank());
    } catch (IllegalAccessException | InvocationTargetException | RuntimeException e) {
      return Optional.empty();
    }
  }
}
