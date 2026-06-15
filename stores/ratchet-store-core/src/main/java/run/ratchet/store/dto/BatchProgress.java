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
package run.ratchet.store.dto;

import java.util.UUID;
import run.ratchet.store.entity.JobPayload;

/**
 * Immutable snapshot of batch progress returned by atomic increment operations.
 *
 * <p>Captures the exact state of a batch at the moment of an atomic increment, enabling progress
 * hooks to be called with accurate, non-duplicated progress values even when multiple child jobs
 * complete simultaneously.
 */
public record BatchProgress(
    UUID batchId, int totalItems, int completedItems, int failedItems, JobPayload progressHook) {

  /** Returns true if the batch has finished processing all items (success or failure). */
  public boolean isComplete() {
    return completedItems + failedItems == totalItems;
  }

  /** Returns the completion percentage (0-100). */
  public int percentComplete() {
    if (totalItems == 0) {
      return 100;
    }
    return (completedItems + failedItems) * 100 / totalItems;
  }
}
