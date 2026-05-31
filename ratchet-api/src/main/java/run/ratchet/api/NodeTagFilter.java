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
package run.ratchet.api;

import java.util.List;

/**
 * Immutable tag affinity filter for a scheduler node.
 *
 * <ul>
 *   <li>{@code requireTags} (inclusive / OR): only claim jobs that have at least one matching tag.
 *       Empty list means no require-filter.
 *   <li>{@code excludeTags} (exclusive): never claim jobs that carry any of these tags. Empty list
 *       means no exclude-filter.
 * </ul>
 *
 * <p>Both lists empty ({@link #NONE}) means the node accepts all jobs.
 */
@Incubating
public record NodeTagFilter(List<String> requireTags, List<String> excludeTags) {

  public static final NodeTagFilter NONE = new NodeTagFilter(List.of(), List.of());

  public NodeTagFilter {
    requireTags = List.copyOf(requireTags);
    excludeTags = List.copyOf(excludeTags);
  }

  /** Returns {@code true} when both lists are empty (no filtering at all). */
  public boolean isUnfiltered() {
    return requireTags.isEmpty() && excludeTags.isEmpty();
  }
}
