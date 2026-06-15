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
package run.ratchet.store.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;

/**
 * Pause / resume transitions for executable jobs.
 *
 * <p>Recurring-master pause/resume lives on {@link RecurringJobStore}.
 */
@Incubating
public interface JobPauseStore {

  /**
   * Atomically transitions to PAUSED, recording the original status for later resume in the same
   * operation to avoid TOCTOU gaps. Returns {@code false} when {@code expected} is WAITING or
   * terminal. Throws {@link IllegalArgumentException} when {@code expected} is PAUSED. Transaction
   * attribute: {@code REQUIRED}.
   */
  boolean transitionToPaused(UUID id, JobStatus expected);

  /**
   * Atomically transitions from PAUSED to the target status, clearing the stored paused-from
   * status. The target must be a non-PAUSED, non-WAITING live status. Transaction attribute: {@code
   * REQUIRED}.
   */
  boolean transitionFromPaused(UUID id, JobStatus target);

  /**
   * Atomically transitions from PAUSED to the stored paused-from status, reading the target from
   * the database row in the same operation to avoid TOCTOU races.
   *
   * @apiNote This method intentionally returns a bare reference rather than {@link
   *     java.util.Optional} so the resume hot path avoids an allocation on every retry. Callers
   *     must null-check the result; the {@link Nullable} annotation reflects this contract for
   *     static analysers.
   * @param id job id to resume
   * @return the status restored from the paused row, or {@code null} when no row is currently
   *     PAUSED. Callers should treat {@code null} as a lost race or missing job and re-read before
   *     deciding whether to retry.
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  @Nullable JobStatus transitionFromPausedAtomic(UUID id);
}
