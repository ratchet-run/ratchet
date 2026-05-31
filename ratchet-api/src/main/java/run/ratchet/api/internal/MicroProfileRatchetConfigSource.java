/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.api.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import run.ratchet.spi.RatchetConfigSource;

/**
 * Optional MicroProfile Config adapter loaded reflectively when MP Config is present.
 *
 * <p><b>Framework-internal:</b> applications must not depend on this class. Used by {@link
 * run.ratchet.api.RatchetOptionsFactory#fromEnvironment} to prepend MicroProfile Config to the
 * ambient configuration chain when MP Config is on the classpath.
 *
 * @since 0.1.0
 */
public final class MicroProfileRatchetConfigSource implements RatchetConfigSource {

  private static final Logger LOG =
      Logger.getLogger(MicroProfileRatchetConfigSource.class.getName());

  private final Object config;
  private final Method getOptionalValue;

  private MicroProfileRatchetConfigSource(Object config, Method getOptionalValue) {
    this.config = config;
    this.getOptionalValue = getOptionalValue;
  }

  public static Optional<RatchetConfigSource> create() {
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
      LOG.log(Level.WARNING, e, () -> "MicroProfile Config is present but could not be loaded");
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
      LOG.log(Level.FINE, e, () -> "MicroProfile Config lookup failed for property: " + name);
      return Optional.empty();
    }
  }
}
