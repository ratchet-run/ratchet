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

/** Raw configuration source used by {@link RatchetConfig}. */
@Incubating
public interface RatchetConfigSource {

  /**
   * Returns a raw configuration value for a property/env pair.
   *
   * <p>When both names are supplied and both exist, lookup precedence is implementation-defined.
   * Callers must order their configured sources rather than depending on intra-source precedence.
   *
   * @param propertyName dotted property name, for example {@code ratchet.poller.batch-size}; {@code
   *     null} or blank values are treated as absent
   * @param environmentVariable environment variable fallback, for example {@code
   *     RATCHET_POLLER_BATCH_SIZE}; {@code null} or blank values are treated as absent
   * @return raw value when present, otherwise {@link Optional#empty()}
   * @throws RuntimeException if the source cannot be read; callers may continue to the next source
   */
  Optional<String> get(String propertyName, String environmentVariable);
}
