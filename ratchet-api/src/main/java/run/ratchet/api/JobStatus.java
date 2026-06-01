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

/**
 * Lifecycle states for a scheduled job.
 *
 * <pre>
 *   PAUSED &lt;-&gt; PENDING -&gt; RUNNING -&gt; SUCCEEDED
 *                |           |
 *                v           v
 *              WAITING     FAILED -&gt; PENDING (retry)
 *                |  ^
 *   signal -----+  +----- timeout -&gt; FAILED
 *
 *   PENDING/RUNNING/WAITING/PAUSED -&gt; CANCELED
 * </pre>
 */
public enum JobStatus {
  /** Visible to polling queries when scheduled_time &lt;= now. */
  PENDING,

  /** Exclusively owned by the executing node; protected via optimistic locking. */
  RUNNING,

  /** Terminal. Eligible for archival after retention period. */
  SUCCEEDED,

  /**
   * Terminal per enum snapshot. Transitions back to PENDING via the retry path when attempts &lt;
   * maxRetries — driven by external logic, not the enum value itself.
   */
  FAILED,

  /** Terminal. No further execution. */
  CANCELED,

  /** NOT visible to polling queries. Transitions back to PENDING when resumed. */
  PAUSED,

  /**
   * Blocked waiting for a named external signal. NOT visible to polling queries. Transitions to
   * PENDING when a signal is delivered via {@code JobSchedulerService.deliverSignal()}, or to
   * FAILED when {@code signalTimeout} elapses. WAITING jobs cannot be paused; they can be canceled.
   */
  WAITING;

  /**
   * Returns true for {@link #SUCCEEDED}, {@link #FAILED}, and {@link #CANCELED}. Used by retry and
   * cascade code to avoid silently overwriting a terminal transition.
   */
  public boolean isTerminal() {
    return this == SUCCEEDED || this == FAILED || this == CANCELED;
  }
}
