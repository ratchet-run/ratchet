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
