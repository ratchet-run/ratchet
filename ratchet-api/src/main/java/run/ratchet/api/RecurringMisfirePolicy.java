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

import java.util.Objects;

/**
 * Controls how a recurring schedule handles a backlog of missed cron occurrences.
 *
 * <p>A backlog exists when at least two scheduled occurrences are already due when Ratchet claims
 * the recurring master. A single due occurrence runs normally under every action; this avoids
 * treating ordinary poll latency as a misfire.
 *
 * @param action action to take when a backlog is detected
 * @param maxCatchUpExecutions maximum total overdue occurrences to create for {@link
 *     Action#CATCH_UP}; must be zero for the other actions
 */
@Incubating
public record RecurringMisfirePolicy(Action action, int maxCatchUpExecutions) {

  /**
   * Existing Ratchet releases create the oldest due occurrence plus ten additional occurrences
   * after downtime. Eleven preserves that behavior while making the total explicit.
   */
  public static final int DEFAULT_MAX_CATCH_UP_EXECUTIONS = 11;

  public RecurringMisfirePolicy {
    Objects.requireNonNull(action, "action must not be null");
    if (action == Action.CATCH_UP && maxCatchUpExecutions < 1) {
      throw new IllegalArgumentException("maxCatchUpExecutions must be >= 1 for CATCH_UP");
    }
    if (action != Action.CATCH_UP && maxCatchUpExecutions != 0) {
      throw new IllegalArgumentException(
          "maxCatchUpExecutions must be 0 unless the action is CATCH_UP");
    }
  }

  /** Returns the default policy: catch up at most eleven overdue occurrences. */
  public static RecurringMisfirePolicy defaults() {
    return catchUp(DEFAULT_MAX_CATCH_UP_EXECUTIONS);
  }

  /** Discards every overdue occurrence when a backlog is detected. */
  public static RecurringMisfirePolicy skip() {
    return new RecurringMisfirePolicy(Action.SKIP, 0);
  }

  /** Creates only the oldest overdue occurrence when a backlog is detected. */
  public static RecurringMisfirePolicy fireOnce() {
    return new RecurringMisfirePolicy(Action.FIRE_ONCE, 0);
  }

  /**
   * Creates up to {@code maxExecutions} overdue occurrences, in scheduled order, then discards any
   * remaining backlog.
   */
  public static RecurringMisfirePolicy catchUp(int maxExecutions) {
    return new RecurringMisfirePolicy(Action.CATCH_UP, maxExecutions);
  }

  /** Action applied when more than one cron occurrence is already due. */
  public enum Action {
    /** Discard every overdue occurrence and resume at the next future cron time. */
    SKIP,

    /** Create only the oldest overdue occurrence, then resume at the next future cron time. */
    FIRE_ONCE,

    /** Create a bounded number of overdue occurrences in scheduled order. */
    CATCH_UP
  }
}
