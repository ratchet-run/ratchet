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
