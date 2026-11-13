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

import java.util.Optional;

/** Stable read access to active business-key reservations. */
public interface BusinessKeyLookup {

  /**
   * Returns the active holder of a business key: a job or a recurring master. Reads the same
   * reservation that {@link JobBuilder#submit()} checks, so it sees jobs and recurring masters
   * alike. {@link JobQueryService#findJobs} sees recurring masters only when the filter's types are
   * exactly RECURRING.
   *
   * <p>Key handling matches {@link JobBuilder#withBusinessKey}. Null or blank keys return empty.
   * The key is trimmed. More than 255 characters or a character outside printable ASCII (U+0020 to
   * U+007E) throws {@link IllegalArgumentException}.
   *
   * <p>Status follows {@link BusinessKeyHolder}: jobs have their live status, and recurring masters
   * are PAUSED when paused and PENDING otherwise. If a reservation's owner row is not visible to
   * the read, such as a store's own write in progress or MongoDB between its two reads, returns
   * empty rather than a holder without a status.
   *
   * <p>This is a point-in-time read. A later {@code submit()} can still lose a race. Treat the
   * answer as a hint or pre-check, not a lock.
   *
   * <p>Joins the caller's transaction if one exists and never starts one that outlives the call.
   * Finding a holder, finding none, or rejecting an invalid key never marks the caller's
   * transaction rollback-only. A store failure throws and, as with {@code submit()}, can leave it
   * rollback-only. Inside a caller transaction this read sees the caller's own uncommitted
   * submissions.
   *
   * <p>On MySQL under the default REPEATABLE READ, the transaction's first plain read fixes its
   * snapshot. A holder that commits or ends later is not seen. Call outside a transaction or use
   * READ COMMITTED for the latest state.
   *
   * <p>No per-job authorization check is performed, matching {@code submit()}, which reports a held
   * key to any caller.
   *
   * @param businessKey the business key to inspect
   * @return the active holder, or empty when no live owner is visible
   * @throws IllegalArgumentException if the trimmed key is too long or contains a character outside
   *     printable ASCII
   */
  Optional<BusinessKeyHolder> findBusinessKeyHolder(String businessKey);
}
