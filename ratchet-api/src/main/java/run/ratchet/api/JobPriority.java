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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Job priority levels for the scheduler. Higher persisted codes indicate higher priority.
 *
 * <p>The explicit {@linkplain #persistedCode() persisted code}, rather than the enum ordinal, is
 * stored in the database. Existing codes must never be changed or reused.
 *
 * <p>Current persisted mapping:
 *
 * <ul>
 *   <li>{@link #LOWEST} = 0
 *   <li>{@link #LOW} = 1
 *   <li>{@link #NORMAL} = 2 (default)
 *   <li>{@link #HIGH} = 3
 *   <li>{@link #CRITICAL} = 4
 * </ul>
 *
 * <p>New priorities must use a previously unused code. Because the stored value also defines
 * scheduling order, a priority above {@link #CRITICAL} must use a code greater than {@code 4} and
 * extend the stores' priority check constraints in the same change.
 */
public enum JobPriority {
  LOWEST(0),
  LOW(1),
  NORMAL(2),
  HIGH(3),
  CRITICAL(4);

  private static final Map<Integer, JobPriority> BY_PERSISTED_CODE = indexPersistedCodes();

  private final int persistedCode;

  JobPriority(int persistedCode) {
    this.persistedCode = persistedCode;
  }

  /** Returns the stable integer stored for this priority. */
  public int persistedCode() {
    return persistedCode;
  }

  /**
   * Returns the priority assigned to a stored code.
   *
   * @throws IllegalArgumentException when the code is not assigned to a priority
   */
  public static JobPriority fromPersistedCode(int persistedCode) {
    JobPriority priority = BY_PERSISTED_CODE.get(persistedCode);
    if (priority == null) {
      throw new IllegalArgumentException("Unknown persisted JobPriority code: " + persistedCode);
    }
    return priority;
  }

  /** Returns the priority assigned to a stored code, or an empty value for an unknown code. */
  public static Optional<JobPriority> findByPersistedCode(int persistedCode) {
    return Optional.ofNullable(BY_PERSISTED_CODE.get(persistedCode));
  }

  private static Map<Integer, JobPriority> indexPersistedCodes() {
    Map<Integer, JobPriority> priorities = new HashMap<>();
    for (JobPriority priority : values()) {
      JobPriority duplicate = priorities.put(priority.persistedCode, priority);
      if (duplicate != null) {
        throw new IllegalStateException(
            "Duplicate persisted JobPriority code "
                + priority.persistedCode
                + " for "
                + duplicate
                + " and "
                + priority);
      }
    }
    return Map.copyOf(priorities);
  }
}
