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

/** Stable read access to the job behind an idempotency-key ledger entry. */
public interface IdempotencyKeyLookup {

  /**
   * Returns the job held by an idempotency key. Reads the same ledger that {@link
   * JobBuilder#submit()} consults. The ledger keeps the key after archive or purge. The job is read
   * from live storage first, then from the archive when the store has that capability.
   *
   * <p>Key handling matches {@link JobBuilder#withIdempotencyKey}. Null or blank keys return empty.
   * The key is trimmed. More than 36 characters throws {@link IllegalArgumentException}.
   *
   * <p>Returns empty when the key has never been used. Otherwise {@link IdempotencyKeyHolder}
   * reports LIVE, ARCHIVED, or PURGED. A resubmit still returns the original id and runs nothing.
   * {@link JobQueryService#findJobs} with {@link JobFilter#idempotencyKey()} sees live jobs only;
   * this lookup also covers archived and purged jobs.
   *
   * <p>This is a point-in-time read. A later {@code submit()} can still race. Treat the answer as a
   * hint or pre-check, not a lock.
   *
   * <p>The call never starts a transaction that outlives it. Finding a holder, finding none, or
   * rejecting an invalid key never marks the caller's transaction rollback-only. A store failure
   * throws and, as with {@code submit()}, can leave it rollback-only.
   *
   * <p>Whether the read joins a caller's transaction depends on the store. The bundled SQL stores
   * join it, so the read sees the caller's own uncommitted submissions. The MongoDB store does not.
   * A MongoDB submission commits in its own session and is not rolled back with the caller's
   * transaction, and this read sees only committed data.
   *
   * <p>On MySQL under the default REPEATABLE READ, the transaction's first plain read fixes its
   * snapshot. The read does not see a holder that commits after that point. It can still return an
   * older state for a job that changes after that point, because the change is not visible until a
   * fresh snapshot. Call outside a transaction or use READ COMMITTED for the latest state.
   *
   * <p>No per-job authorization check is performed, matching {@code submit()}, which reports a held
   * key to any caller.
   *
   * @param idempotencyKey the idempotency key to inspect
   * @return the holder, or empty when the ledger has no entry
   * @throws IllegalArgumentException if the trimmed key is longer than 36 characters
   */
  Optional<IdempotencyKeyHolder> findIdempotencyKeyHolder(String idempotencyKey);
}
