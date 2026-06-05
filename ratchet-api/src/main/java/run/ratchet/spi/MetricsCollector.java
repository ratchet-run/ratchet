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
package run.ratchet.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;

/**
 * Receives job lifecycle events (start, success, failure) for metrics collection. Additional
 * callbacks may be added in future releases.
 */
@Incubating
public interface MetricsCollector {

  /** Called when a job starts execution. */
  void jobStarted(UUID jobId, JobType type, JobPriority priority);

  /**
   * Called when a job completes successfully.
   *
   * @param jobId job that completed; never {@code null}
   * @param type public job type; never {@code null}
   * @param executionTimeMs wall-clock duration of the completed attempt
   */
  void jobCompleted(UUID jobId, JobType type, long executionTimeMs);

  /**
   * Called when a job fails.
   *
   * @param jobId job that failed; never {@code null}
   * @param type public job type; never {@code null}
   * @param cause failure that ended the attempt; never {@code null}
   * @param attempt the 1-based attempt number including this failure
   */
  void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt);

  /** Called when a successful execution must retry a transient store finalization conflict. */
  void successFinalizationRetried(UUID jobId, JobType type);

  /**
   * Called when a successful execution falls back to a minimal terminal success write after
   * exhausting full-result finalization retries.
   */
  void successFinalizationMinimal(UUID jobId, JobType type);

  /**
   * Called when a successful execution cannot persist either full or minimal success due to
   * repeated transient store conflicts and must be left RUNNING for later recovery.
   */
  void successFinalizationStuck(UUID jobId, JobType type);

  /**
   * Called when the poller hits a transient store conflict while claiming work for an execution
   * role.
   */
  void claimTransientFailure(String executionType);

  /** Called after the poller claims executable work for a specific execution role. */
  void jobsClaimed(String executionType, int claimedCount);

  /** Called when a submission gate blocks local execution of a claimed job. */
  void gateRejected(String executionType, String gateStatus);

  /** Called when the local node directly wakes its poller in response to new work. */
  void localWakeup(String source);

  /**
   * Called when a job's requested execution target could not be honored and the router fell back to
   * a different pool. A high count signals a misconfiguration, such as {@code
   * default-threading-mode=virtual} with no virtual executor configured.
   *
   * <p>{@code effective} is always {@code "platform"} today — the platform pool is the only
   * fallback target. The contract leaves room for other pool names when cascading fallbacks land,
   * so implementations should not assume a fixed value.
   *
   * @param requested the execution target the job asked for
   * @param effective the pool the job actually ran on; currently always {@code "platform"}
   */
  default void executionTargetFallback(String requested, String effective) {
    // default no-op
  }

  /**
   * Called when a cluster wakeup publish attempt is observed.
   *
   * @param transport cluster transport, e.g. {@code jms}
   * @param outcome publish outcome, e.g. {@code success}, {@code failure}, {@code skipped}
   */
  default void clusterWakeupPublished(String transport, String outcome) {
    // default no-op
  }

  /**
   * Called when a cluster wakeup message is observed on the receiving side.
   *
   * @param transport cluster transport, e.g. {@code jms}
   * @param outcome receive outcome, e.g. {@code delivered}, {@code ignored_self}
   */
  default void clusterWakeupReceived(String transport, String outcome) {
    // default no-op
  }

  /**
   * Called when a lifecycle callback ({@code onSuccess}/{@code onFailure}) throws. Callback
   * failures never fail the parent job. Default is a no-op.
   */
  default void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    // default no-op
  }

  /** Called when a job is created in WAITING status for an external signal. */
  default void signalWaiting(UUID jobId, JobType type, String signalKey) {
    // default no-op
  }

  /** Called after a signal delivery transitions a job from WAITING to PENDING. */
  default void signalDelivered(
      UUID jobId, JobType type, String signalKey, SignalDecision.Outcome outcome) {
    // default no-op
  }

  /** Called when a signal-waiting job times out. */
  default void signalTimedOut(UUID jobId, JobType type, String signalKey) {
    // default no-op
  }

  /** Called when a signal-waiting job is cancelled before delivery. */
  default void signalCancelled(UUID jobId, JobType type, String signalKey) {
    // default no-op
  }

  /**
   * Called when the store finishes a timed operation on the hot path.
   *
   * <p>Store implementations should cover claim lookup and claim marking, direct pickup, terminal
   * success/failure/cancel/retry transitions, running-job reset, recurring pause/resume and
   * cancellation, and other store-specific write paths that can dominate poller or executor
   * latency. Implementations should use stable operation names and classify outcomes as {@code
   * updated}, {@code miss}, {@code empty}, {@code success}, {@code transient_failure}, or {@code
   * failure} where those labels fit the operation.
   *
   * @param store backend/store identifier, e.g. {@code mysql}
   * @param operation logical operation, e.g. {@code claim_lookup} or {@code mark_succeeded}
   * @param outcome outcome label, e.g. {@code success}, {@code miss}, or {@code transient_failure}
   * @param durationNanos elapsed wall-clock time in nanoseconds
   */
  default void storeOperation(String store, String operation, String outcome, long durationNanos) {
    // default no-op
  }

  /**
   * Called when the poller updates its claim-path circuit breaker state.
   *
   * @param breakerName logical breaker identifier, e.g. {@code store.claim}
   * @param state breaker state enum name, e.g. {@code CLOSED} or {@code OPEN}
   */
  default void pollerBreakerState(String breakerName, String state) {
    // default no-op
  }

  /**
   * Called when a row flagged {@code encrypted_payload} is read back as unframed plaintext (ADR
   * Q-D). An operational-integrity signal — a write-time downgrade, an un-upgraded node, or a bug —
   * never a read failure. The read still succeeds with the plaintext value.
   *
   * @param jobId the affected job or recurring master
   * @param surface the protected surface that read back unframed, e.g. {@code PAYLOAD_ARGS}
   */
  default void encryptionIntegrityViolation(UUID jobId, String surface) {
    // default no-op
  }
}
