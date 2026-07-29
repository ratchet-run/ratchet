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
package org.eclipse.microprofile.config;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Minimal MicroProfile ConfigProvider test fixture for the reflection-only adapter. */
public final class ConfigProvider {

  private static final TestConfig CONFIG = new TestConfig();

  private ConfigProvider() {}

  public static Config getConfig() {
    return CONFIG;
  }

  public static void setValue(String propertyName, String value) {
    CONFIG.values.put(propertyName, value);
  }

  public static void clear() {
    CONFIG.values.clear();
  }

  public static final class TestConfig implements Config {

    private final Map<String, String> values = new ConcurrentHashMap<>();

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
      if (!String.class.equals(propertyType)) {
        return Optional.empty();
      }
      return Optional.ofNullable(values.get(propertyName)).map(propertyType::cast);
    }
  }
}
