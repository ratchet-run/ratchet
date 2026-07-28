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

/**
 * Platform-supplied source for the current caller principal.
 *
 * <p>The reference implementation consults platform sources after the application-configured {@link
 * CallerPrincipalResolver} and the bound {@code JobContext} principal. Runtime integrations use
 * this seam to bridge their native security identity without making the core engine compile against
 * a specific security API.
 */
@Incubating
@FunctionalInterface
public interface PrincipalSource {

  /**
   * Resolves the current platform principal.
   *
   * @return the current principal name, or {@link Optional#empty()} if no principal is available
   */
  Optional<String> currentPrincipal();
}
