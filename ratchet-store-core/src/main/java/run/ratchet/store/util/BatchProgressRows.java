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

import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.JobPayload;

/** Maps SQL batch progress rows with columns completed, failed, total, progress hook. */
public final class BatchProgressRows {

  private BatchProgressRows() {}

  public static BatchProgress fromCurrentRow(
      UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
    requireProgressRow(row);
    return progress(
        batchId,
        totalItems(row),
        completedItems(row),
        failedItems(row),
        row[3],
        progressHookParser);
  }

  public static BatchProgress afterCompletedIncrement(
      UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
    requireProgressRow(row);
    return progress(
        batchId,
        totalItems(row),
        completedItems(row) + 1,
        failedItems(row),
        row[3],
        progressHookParser);
  }

  public static BatchProgress afterFailedIncrement(
      UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
    requireProgressRow(row);
    return progress(
        batchId,
        totalItems(row),
        completedItems(row),
        failedItems(row) + 1,
        row[3],
        progressHookParser);
  }

  public static int completedItems(Object[] row) {
    requireProgressRow(row);
    return intValue(row[0]);
  }

  public static int failedItems(Object[] row) {
    requireProgressRow(row);
    return intValue(row[1]);
  }

  private static int totalItems(Object[] row) {
    return intValue(row[2]);
  }

  private static BatchProgress progress(
      UUID batchId,
      int totalItems,
      int completedItems,
      int failedItems,
      Object progressHook,
      Function<Object, JobPayload> progressHookParser) {
    Objects.requireNonNull(progressHookParser, "progressHookParser");
    JobPayload parsedHook;
    try {
      parsedHook = progressHookParser.apply(progressHook);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not parse batch progress hook", e);
    }
    return new BatchProgress(batchId, totalItems, completedItems, failedItems, parsedHook);
  }

  private static int intValue(Object value) {
    if (value == null) {
      throw new IllegalStateException("Batch progress counter column is null");
    }
    if (!(value instanceof Number number)) {
      throw new IllegalStateException(
          "Batch progress counter column is not numeric: " + value.getClass().getName());
    }
    return number.intValue();
  }

  private static void requireProgressRow(Object[] row) {
    if (row == null || row.length < 4) {
      int length = row == null ? 0 : row.length;
      throw new IllegalArgumentException(
          "Batch progress row must contain completed, failed, total, and progress hook columns; got "
              + length);
    }
  }
}
