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
package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/** Typed runtime configuration facade used to build {@code RatchetOptions}. */
@Incubating
public interface RatchetConfig {

  /**
   * Reads and parses a typed configuration value.
   *
   * <p>Missing, blank, or invalid raw values return the key default rather than {@code null}.
   *
   * @param key configuration key to read; never {@code null}
   * @return parsed value or {@link RatchetConfigKey#defaultValue()}; never {@code null}
   */
  <T> T get(RatchetConfigKey<T> key);

  /**
   * Reads the first raw value available for a key before parsing.
   *
   * @param key configuration key to read; never {@code null}
   * @return raw string value from the configured sources, or {@link Optional#empty()} when absent
   */
  Optional<String> raw(RatchetConfigKey<?> key);
}
