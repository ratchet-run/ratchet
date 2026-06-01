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
package run.ratchet.ri.core.internal;

import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.NodeTagAffinityProvider;

/**
 * Default {@link NodeTagAffinityProvider} that reads {@code requireTags} / {@code excludeTags} from
 * {@link RatchetOptions.NodeOptions} at construction time and returns the same filter on every
 * call. Produces {@link NodeTagFilter#NONE} when no tags are configured.
 */
public record DefaultNodeTagAffinityProvider(NodeTagFilter tagFilter)
    implements NodeTagAffinityProvider {

  public DefaultNodeTagAffinityProvider(RatchetOptions tagFilter) {
    this(tagFilter.node().tagFilter());
  }
}
