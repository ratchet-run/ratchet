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

import run.ratchet.api.Incubating;
import run.ratchet.api.NodeTagFilter;

/**
 * SPI for per-node tag affinity. Called once per poll cycle so the filter can change at runtime
 * (e.g., based on hardware availability or load).
 *
 * <p>Return {@link NodeTagFilter#NONE} to disable filtering entirely. The default CDI bean ({@code
 * DefaultNodeTagAffinityProvider}) reads {@code requireTags} / {@code excludeTags} from {@link
 * run.ratchet.api.RatchetOptions.NodeOptions} at construction time.
 */
@Incubating
public interface NodeTagAffinityProvider {

  /**
   * Returns the node's current tag filter.
   *
   * @return filter to apply to claim queries; return {@link NodeTagFilter#NONE} to claim all
   *     eligible jobs. Never return {@code null}.
   */
  NodeTagFilter tagFilter();
}
