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
package run.ratchet.api.exception;

import java.io.Serial;

/**
 * Thrown on concurrent version mismatch (optimistic-lock conflict) detected by a {@code JobStore}
 * mutation. Typical sources are claim, status-transition, heartbeat, and update-attempts operations
 * that compare the stored row version against the value held by the caller.
 *
 * <p>This is intentionally a {@link RuntimeException} rather than a {@link
 * jakarta.persistence.PersistenceException} so that it does NOT mark the surrounding JTA
 * transaction for rollback. Callers may inspect the error and react without losing other work in
 * the same transaction.
 *
 * <p>Distinct from {@link RatchetTransientStoreException}: a transient store failure is a retry-
 * worthy infrastructure error (connection blip, deadlock loser) where the same logical operation is
 * expected to succeed on retry. An optimistic-lock conflict means another actor changed the row;
 * the correct response is usually to re-read the current state and decide whether to retry, skip,
 * or surface the conflict. Internal RI code only retries an optimistic-lock failure on
 * non-state-changing paths (such as the heartbeat refresh).
 */
public class RatchetOptimisticLockException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public RatchetOptimisticLockException(String message) {
    super(message);
  }

  public RatchetOptimisticLockException(String message, Throwable cause) {
    super(message, cause);
  }
}
