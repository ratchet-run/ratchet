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

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.exception.RatchetTransientStoreException;

/**
 * Terminal status transitions for jobs: success / failure / cancel.
 *
 * <p>Implementations are expected to flip a live job to its terminal form atomically (for the
 * hot/cold MySQL store, this means hot DELETE + cold UPDATE + bkres DELETE in a single
 * transaction).
 *
 * <p>A {@code false} return means the requested transition did not match a row. Store failures
 * propagate as {@link RatchetTransientStoreException}; implementations must not convert them to
 * {@code false}.
 */
@Incubating
public interface JobTerminalStore {

  /**
   * Marks a job as succeeded with a stored result.
   *
   * @param id job id to transition; must be a currently-RUNNING job
   * @param resultJson serialized result payload to persist, or {@code null} to omit
   * @param resultType fully-qualified Java type name of the result, or {@code null} when {@code
   *     resultJson} is {@code null}
   * @param start execution start instant captured by the worker; never {@code null}
   * @param end execution end instant captured by the worker; never {@code null}
   * @param durationMs total execution duration in ms (typically {@code end - start}), or {@code
   *     null} when the worker did not record it
   * @param queueWaitMs queue-wait latency in ms (scheduled-time → claim-time), or {@code null} when
   *     the store cannot compute it
   * @return {@code true} when the live row transitioned to SUCCEEDED, {@code false} when no
   *     matching RUNNING row was found
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobSucceeded(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs);

  /**
   * Marks a job as succeeded without a stored result.
   *
   * @param id job id to transition; must be a currently-RUNNING job
   * @param start execution start instant captured by the worker; never {@code null}
   * @param end execution end instant captured by the worker; never {@code null}
   * @param durationMs total execution duration in ms, or {@code null} when not recorded
   * @param queueWaitMs queue-wait latency in ms, or {@code null} when not computed
   * @return {@code true} when the live row transitioned to SUCCEEDED, {@code false} when no
   *     matching RUNNING row was found
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs);

  /**
   * Marks a batch child as succeeded and advances the parent batch counters atomically.
   *
   * <p>The child terminal transition and parent counter update must happen in the same transaction;
   * implementations must not commit the counter in an inner {@code REQUIRES_NEW} transaction.
   *
   * @param jobId batch-child job id to transition; must be a currently-RUNNING child
   * @param resultJson serialized result payload to persist, or {@code null} to omit
   * @param resultType fully-qualified Java type name of the result, or {@code null} when {@code
   *     resultJson} is {@code null}
   * @param start execution start instant captured by the worker; never {@code null}
   * @param end execution end instant captured by the worker; never {@code null}
   * @param durationMs total execution duration in ms, or {@code null} when not recorded
   * @param queueWaitMs queue-wait latency in ms, or {@code null} when not computed
   * @param batchId batch parent id whose counters are advanced in the same transaction; never
   *     {@code null}
   * @return {@code true} when both the child transition and the parent counter update succeeded,
   *     {@code false} when no matching RUNNING child row was found
   * @throws RatchetTransientStoreException if either update cannot complete
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobSucceededAndUpdateBatch(
      UUID jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      UUID batchId);

  /**
   * Atomically transitions a RUNNING job to terminal FAILED state. Captures total attempts and
   * terminal error in a single store call. Replaces the older {@code setStatus(FAILED)+save}
   * pattern that is incompatible with the hot/cold split (hot DELETE + cold UPDATE + bkres DELETE
   * in one tx).
   *
   * <p><b>Caller contract:</b> the target job must be in {@code RUNNING} status when this is
   * invoked. Implementations may match the hot row on {@code status = 'RUNNING'} and return {@code
   * false} (silent no-op) for any other status, including {@code WAITING}. To fail a WAITING job
   * (e.g. on signal timeout), use {@code compareAndSwapStatus} with an expected status of {@code
   * WAITING}, which dispatches to the matching {@code WAITING}-aware path internally.
   *
   * @param id job id to transition; must be a currently-RUNNING job
   * @param terminalError final error message to persist, or {@code null} when none is available
   * @param totalAttempts total attempts the worker recorded across all retries
   * @return {@code true} when the live row transitioned to FAILED, {@code false} for any other
   *     status (silent no-op for non-RUNNING rows)
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts);

  /**
   * Cancels a job by id. Dispatches by job_type internally: executable jobs DELETE the live queue
   * row + UPDATE cold to terminal CANCELED; recurring masters clear the recurring shim and set cold
   * terminal CANCELED. Single-table store implementations may treat this as an UPDATE to CANCELED.
   * Returns true iff the job transitioned to CANCELED.
   *
   * @param id job id to cancel
   * @return {@code true} when the job transitioned to CANCELED, {@code false} when the job did not
   *     exist or was already terminal
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean cancelJob(UUID id);
}
