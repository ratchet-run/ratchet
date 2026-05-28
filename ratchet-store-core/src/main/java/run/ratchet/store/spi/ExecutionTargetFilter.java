package run.ratchet.store.spi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/** Predicate for claim queries that need to scope rows to an effective executor pool. */
public final class ExecutionTargetFilter {

  private static final ExecutionTargetFilter ANY =
      new ExecutionTargetFilter(List.of(), false, false, true);

  private final List<String> explicitTargets;
  private final boolean includeNull;
  private final boolean exclusion;
  private final boolean matchAll;

  private ExecutionTargetFilter(
      List<String> explicitTargets, boolean includeNull, boolean exclusion, boolean matchAll) {
    this.explicitTargets = explicitTargets;
    this.includeNull = includeNull;
    this.exclusion = exclusion;
    this.matchAll = matchAll;
  }

  /** Matches every row, preserving the pre-target-aware claim behavior. */
  public static ExecutionTargetFilter any() {
    return ANY;
  }

  /** Matches rows whose target is in {@code explicitTargets}, plus null targets when requested. */
  public static ExecutionTargetFilter matching(
      Collection<String> explicitTargets, boolean includeNull) {
    Objects.requireNonNull(explicitTargets, "explicitTargets");
    LinkedHashSet<String> normalized = new LinkedHashSet<>();
    for (String target : explicitTargets) {
      if (target == null || target.isBlank()) {
        throw new IllegalArgumentException("execution target must not be blank");
      }
      normalized.add(target);
    }
    return new ExecutionTargetFilter(
        Collections.unmodifiableList(new ArrayList<>(normalized)), includeNull, false, false);
  }

  /**
   * Matches rows whose non-null target is not in {@code excludedTargets}, plus null targets when
   * requested.
   */
  public static ExecutionTargetFilter excluding(
      Collection<String> excludedTargets, boolean includeNull) {
    Objects.requireNonNull(excludedTargets, "excludedTargets");
    LinkedHashSet<String> normalized = new LinkedHashSet<>();
    for (String target : excludedTargets) {
      if (target == null || target.isBlank()) {
        throw new IllegalArgumentException("execution target must not be blank");
      }
      normalized.add(target);
    }
    if (normalized.isEmpty() && includeNull) {
      return any();
    }
    return new ExecutionTargetFilter(
        Collections.unmodifiableList(new ArrayList<>(normalized)), includeNull, true, false);
  }

  public boolean isAny() {
    return matchAll;
  }

  public boolean matchesNothing() {
    return !matchAll && !exclusion && explicitTargets.isEmpty() && !includeNull;
  }

  public List<String> explicitTargets() {
    return exclusion ? List.of() : explicitTargets;
  }

  public List<String> excludedTargets() {
    return exclusion ? explicitTargets : List.of();
  }

  public boolean includeNull() {
    return includeNull;
  }

  public boolean isExclusion() {
    return exclusion;
  }

  public boolean matches(String executionTarget) {
    if (matchAll) {
      return true;
    }
    if (executionTarget == null) {
      return includeNull;
    }
    if (exclusion) {
      return !explicitTargets.contains(executionTarget);
    }
    return explicitTargets.contains(executionTarget);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ExecutionTargetFilter that)) {
      return false;
    }
    return includeNull == that.includeNull
        && exclusion == that.exclusion
        && matchAll == that.matchAll
        && explicitTargets.equals(that.explicitTargets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(explicitTargets, includeNull, exclusion, matchAll);
  }

  @Override
  public String toString() {
    if (matchAll) {
      return "ExecutionTargetFilter[any]";
    }
    String targetLabel = exclusion ? "excludedTargets=" : "explicitTargets=";
    return "ExecutionTargetFilter["
        + targetLabel
        + explicitTargets
        + ", includeNull="
        + includeNull
        + "]";
  }
}
