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
package run.ratchet.store.util;

import java.util.UUID;

/**
 * Shared row-coercion helpers for hydrating jobs from native-query projections. The per-store row
 * mappers extract identical typed values from the projection; only the dialect label in the failure
 * message differs, so it is supplied per store.
 */
public final class JobHydrationSupport {

  private final String dialectLabel;

  /**
   * @param dialectLabel store name used in hydration failure messages (for example {@code
   *     "MySQL"}).
   */
  public JobHydrationSupport(String dialectLabel) {
    this.dialectLabel = dialectLabel;
  }

  public <E extends Enum<E>> E enumValue(
      Object[] row, int index, String column, Class<E> enumType) {
    String raw = RowValues.stringOrNull(row[index]);
    if (raw == null) {
      throw hydrationFailure(row, index, column, null, null);
    }
    try {
      return Enum.valueOf(enumType, raw);
    } catch (IllegalArgumentException e) {
      throw hydrationFailure(row, index, column, raw, e);
    }
  }

  public <E extends Enum<E>> E enumValueOrNull(
      Object[] row, int index, String column, Class<E> enumType) {
    String raw = RowValues.stringOrNull(row[index]);
    if (raw == null) {
      return null;
    }
    try {
      return Enum.valueOf(enumType, raw);
    } catch (IllegalArgumentException e) {
      throw hydrationFailure(row, index, column, raw, e);
    }
  }

  public Number requiredNumber(Object[] row, int index, String column) {
    Number number = numberOrNull(row, index, column);
    if (number == null) {
      throw hydrationFailure(row, index, column, null, null);
    }
    return number;
  }

  public Number numberOrNull(Object[] row, int index, String column) {
    Object value = row[index];
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number;
    }
    throw hydrationFailure(row, index, column, value, null);
  }

  private JobHydrationException hydrationFailure(
      Object[] row, int index, String column, Object value, Throwable cause) {
    return new JobHydrationException(
        "Failed to hydrate "
            + dialectLabel
            + " job "
            + safeJobId(row)
            + ": column "
            + column
            + " at index "
            + index
            + " has value "
            + value,
        cause);
  }

  private static UUID safeJobId(Object[] row) {
    try {
      return RowValues.uuidOrNull(row[0]);
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  /** Thrown when a native-query projection cannot be mapped to a job entity. */
  public static final class JobHydrationException extends IllegalStateException {
    public JobHydrationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
