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

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import run.ratchet.api.Incubating;
import run.ratchet.api.Nullable;

/**
 * Classifies exceptions for circuit breaker accounting. Class matching includes subclasses and
 * walks the cause chain with identity-based cycle detection. Ignore matches always win.
 *
 * <p>With no record classes or predicate, every exception not ignored is recorded as a failure.
 * Otherwise a record class match or a predicate accepting the original throwable records a failure;
 * other exceptions count as successes. Ignored exceptions count as neither success nor failure.
 * Exceptions are rethrown unchanged by the breaker regardless of their classification.
 *
 * <p>Equality compares the predicate with its own {@code equals}, which is identity for lambdas.
 *
 * @param recordExceptions classes to record as failures anywhere in the cause chain
 * @param ignoreExceptions classes to exclude from accounting anywhere in the cause chain
 * @param recordPredicate optional predicate tested against the original throwable
 */
@Incubating
public record CircuitBreakerExceptionFilter(
    List<Class<? extends Throwable>> recordExceptions,
    List<Class<? extends Throwable>> ignoreExceptions,
    @Nullable Predicate<Throwable> recordPredicate) {

  /** Records every exception as a failure. */
  public static final CircuitBreakerExceptionFilter RECORD_ALL =
      new CircuitBreakerExceptionFilter(List.of(), List.of(), null);

  /** Creates a filter with immutable lists; null lists become empty lists. */
  public CircuitBreakerExceptionFilter {
    recordExceptions = recordExceptions == null ? List.of() : List.copyOf(recordExceptions);
    ignoreExceptions = ignoreExceptions == null ? List.of() : List.copyOf(ignoreExceptions);
  }

  /**
   * Creates a filter from annotation arrays.
   *
   * @param record classes to record, or null for none
   * @param ignore classes to ignore, or null for none
   * @return the exception filter
   */
  public static CircuitBreakerExceptionFilter of(
      Class<? extends Throwable>[] record, Class<? extends Throwable>[] ignore) {
    return new CircuitBreakerExceptionFilter(
        record == null ? List.of() : Arrays.asList(record),
        ignore == null ? List.of() : Arrays.asList(ignore),
        null);
  }

  /**
   * Classifies the throwable using ignore precedence, then record classes or the predicate.
   *
   * @param t the original throwable
   * @return its accounting outcome
   */
  public Outcome classify(Throwable t) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    boolean recorded = false;
    for (Throwable cause = t; cause != null && visited.add(cause); cause = cause.getCause()) {
      for (Class<? extends Throwable> ignored : ignoreExceptions) {
        if (ignored.isInstance(cause)) {
          return Outcome.IGNORED;
        }
      }
      for (Class<? extends Throwable> record : recordExceptions) {
        if (record.isInstance(cause)) {
          recorded = true;
        }
      }
    }
    return isRecordAll() || recorded || (recordPredicate != null && recordPredicate.test(t))
        ? Outcome.RECORDED
        : Outcome.NOT_RECORDED;
  }

  /**
   * Unions class lists and ORs predicates, with ignore matches retaining precedence.
   *
   * @param other the per-call filter, or null
   * @return the merged filter, or this filter when other is null
   */
  public CircuitBreakerExceptionFilter merge(@Nullable CircuitBreakerExceptionFilter other) {
    if (other == null) {
      return this;
    }
    Set<Class<? extends Throwable>> records = new LinkedHashSet<>(recordExceptions);
    records.addAll(other.recordExceptions);
    Set<Class<? extends Throwable>> ignores = new LinkedHashSet<>(ignoreExceptions);
    ignores.addAll(other.ignoreExceptions);
    Predicate<Throwable> predicate = recordPredicate;
    if (predicate == null) {
      predicate = other.recordPredicate;
    } else if (other.recordPredicate != null) {
      predicate = predicate.or(other.recordPredicate);
    }
    return new CircuitBreakerExceptionFilter(List.copyOf(records), List.copyOf(ignores), predicate);
  }

  /**
   * Returns whether all exceptions not ignored are recorded.
   *
   * @return true when no record classes or predicate restrict recording
   */
  public boolean isRecordAll() {
    return recordExceptions.isEmpty() && recordPredicate == null;
  }

  /** Accounting outcome for a thrown exception. */
  public enum Outcome {
    /** Counts as a failure. */
    RECORDED,
    /** Counts as a success even though the exception is rethrown. */
    NOT_RECORDED,
    /** Counts as neither success nor failure. */
    IGNORED
  }
}
