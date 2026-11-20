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

/**
 * Result of an atomic completion; progress is the exact batch child snapshot after the increment.
 *
 * @param durable true only when the store committed its own transaction before returning, so the
 *     caller's transaction cannot roll the write back; false when the write joined the caller's
 *     transaction
 */
public record JobCompletionResult(boolean committed, boolean durable, BatchProgress batchProgress) {
  public static JobCompletionResult notCommitted() {
    return new JobCompletionResult(false, false, null);
  }

  public static JobCompletionResult joined(BatchProgress batchProgress) {
    return new JobCompletionResult(true, false, batchProgress);
  }

  public static JobCompletionResult durable(BatchProgress batchProgress) {
    return new JobCompletionResult(true, true, batchProgress);
  }
}
