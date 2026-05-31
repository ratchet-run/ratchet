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
import run.ratchet.api.JobFilter;
import run.ratchet.api.exception.JobAuthorizationException;

/**
 * SPI for authorizing job operations.
 *
 * <p>This SPI is fully wired into the reference implementation and is called at every mutation
 * entry point ({@link #checkCreate}, {@link #checkCancel}, {@link #checkPause}, {@link
 * #checkResume}, {@link #checkRetry}, {@link #checkDeliverSignal}) and at read entry points ({@link
 * #checkRead}, {@link #filterForPrincipal}). The default reference implementation ({@code
 * PermitAllJobAuthorizationPolicy}) allows every operation. Integrators replace it with a CDI
 * {@code @Alternative @Priority(APPLICATION)} bean to enforce site-specific rules.
 *
 * <h2>Thread-safety</h2>
 *
 * <p>Implementations MUST be thread-safe. A single instance is shared across all executor threads
 * and caller threads.
 *
 * <h2>Null principals</h2>
 *
 * <p>All principal parameters MAY be {@code null}. {@code null} indicates either that no Jakarta
 * Security context was active when the job was created ({@code ownerPrincipal}) or that no security
 * context is active on the current thread ({@code currentPrincipal}). Implementations MUST tolerate
 * {@code null} without throwing {@link NullPointerException}.
 *
 * <h2>Callback execution</h2>
 *
 * <p>{@link #checkExecute} is NOT called for {@code onSuccess}/{@code onFailure} callback payloads.
 * Callbacks are registered at creation time by the same principal that submitted the root job,
 * which is already gated by {@link #checkCreate}.
 *
 * <h2>Bulk recurring cancel</h2>
 *
 * <p>{@link run.ratchet.api.JobSchedulerService#cancelRecurringJobsByTag(String)} and {@link
 * run.ratchet.api.JobSchedulerService#cancelRecurringJobByBusinessKey(String)} are not subject to
 * per-job authorization checks. Use {@link run.ratchet.api.JobSchedulerService#cancelJob(UUID)} for
 * authorization-gated cancellation.
 *
 * @since 0.1
 */
@Incubating
public interface JobAuthorizationPolicy {

  /**
   * Called at direct API job submission, within the {@code REQUIRED} transaction, after the caller
   * principal has been captured but before the job is persisted.
   *
   * <p>Also called for chain steps and workflow branches at their creation points. Batch streaming
   * children are excluded because the root batch submission was already authorized before fan-out.
   *
   * @param jobId the UUIDv7 job identifier (client-generated, not yet persisted)
   * @param callerPrincipal the principal captured at creation; {@code null} if no security context
   *     was active
   * @throws JobAuthorizationException if creation is denied
   */
  void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException;

  /**
   * Called immediately before payload execution, on the executor thread.
   *
   * <p><strong>Important:</strong> the executor thread carries no live {@code CallerPrincipal}.
   * This method receives only the principal captured at job creation ({@code ownerPrincipal}). It
   * is an owner-based revocation hook — useful for denying execution when an account has been
   * deactivated since submission — not a caller-present gate.
   *
   * <p>The default implementation is a no-op (no execution-time check). Override only when
   * principal-revocation semantics are required.
   *
   * @param jobId the job identifier
   * @param ownerPrincipal the principal who submitted the job; {@code null} for system jobs
   * @throws JobAuthorizationException if execution is denied; the job will be marked FAILED and
   *     will NOT be retried ({@link run.ratchet.api.DoNotRetry})
   */
  default void checkExecute(UUID jobId, String ownerPrincipal) throws JobAuthorizationException {}

  /**
   * Called before cancellation, within the {@code REQUIRED} transaction.
   *
   * <p>Note: there is a TOCTOU window between loading {@code ownerPrincipal} and the CAS that
   * actually cancels the job. If the job is deleted in that window, {@code ownerPrincipal} will be
   * {@code null}. Implementations MUST tolerate this.
   *
   * @param jobId the job to cancel
   * @param ownerPrincipal the principal who created the job; {@code null} for system jobs
   * @param currentPrincipal the principal requesting cancellation; {@code null} if no security
   *     context is active
   * @throws JobAuthorizationException if cancellation is denied
   */
  void checkCancel(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException;

  /**
   * Called before pausing a job, within the {@code REQUIRED} transaction.
   *
   * @param jobId the job to pause
   * @param ownerPrincipal the principal who created the job; {@code null} for system jobs
   * @param currentPrincipal the principal requesting the pause; {@code null} if no security context
   *     is active
   * @throws JobAuthorizationException if the pause is denied
   */
  void checkPause(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException;

  /**
   * Called before resuming a paused job, within the {@code REQUIRED} transaction.
   *
   * @param jobId the job to resume
   * @param ownerPrincipal the principal who created the job; {@code null} for system jobs
   * @param currentPrincipal the principal requesting the resume; {@code null} if no security
   *     context is active
   * @throws JobAuthorizationException if the resume is denied
   */
  void checkResume(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException;

  /**
   * Called before retrying a failed job, within the {@code REQUIRED} transaction.
   *
   * <p>Note: same TOCTOU caveat as {@link #checkCancel} — {@code ownerPrincipal} may be {@code
   * null} if the job was deleted between the pre-load and the CAS.
   *
   * @param jobId the job to retry
   * @param ownerPrincipal the principal who created the job; {@code null} for system jobs
   * @param currentPrincipal the principal requesting the retry; {@code null} if no security context
   *     is active
   * @throws JobAuthorizationException if the retry is denied
   */
  void checkRetry(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException;

  /**
   * Called before delivering a signal to a specific job, within the {@code REQUIRED} transaction.
   *
   * <p>Note: same TOCTOU caveat as {@link #checkCancel} — {@code ownerPrincipal} may be {@code
   * null} if the job was deleted between the pre-load and the signal delivery CAS.
   *
   * <p>The default implementation is a no-op for compatibility. Override to enforce who may unblock
   * a WAITING job.
   *
   * @param jobId the job to signal
   * @param ownerPrincipal the principal who created the job; {@code null} for system jobs
   * @param currentPrincipal the principal delivering the signal; {@code null} if no security
   *     context is active
   * @throws JobAuthorizationException if signal delivery is denied
   */
  default void checkDeliverSignal(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {}

  /**
   * Called before bulk delivery of a named signal, within the {@code REQUIRED} transaction.
   *
   * <p>Key-based delivery is an atomic bulk operation, so this hook receives the signal key instead
   * of per-job owner principals. Policies that need owner-scoped authorization should require
   * callers to use the job-id overload.
   *
   * <p>The default implementation is a no-op for compatibility. Override to restrict who may
   * broadcast signals by key.
   *
   * @param signalKey the named signal being delivered
   * @param currentPrincipal the principal delivering the signal; {@code null} if no security
   *     context is active
   * @throws JobAuthorizationException if signal delivery is denied
   */
  default void checkDeliverSignal(String signalKey, String currentPrincipal)
      throws JobAuthorizationException {}

  /**
   * Called before returning a single job's detail to the caller via {@link
   * run.ratchet.api.JobQueryService#getJobDetail}.
   *
   * <p>The default implementation is a no-op (all authenticated callers may read). Override to
   * enforce principal-scoped visibility — for example, to restrict callers to jobs they submitted.
   *
   * @param jobId the job being read
   * @param callerPrincipal the principal requesting the read; {@code null} if no security context
   *     is active
   * @throws JobAuthorizationException if read access is denied
   */
  default void checkRead(UUID jobId, String callerPrincipal) throws JobAuthorizationException {}

  /**
   * Rewrites a {@link JobFilter} to enforce list-level visibility for the given principal.
   *
   * <p>Called by {@link run.ratchet.api.JobQueryService#findJobs} before passing the filter to the
   * store, so that the store query itself is scoped to what the principal may see.
   *
   * <p>Owner-only policies should inject the principal into the filter's {@code callerPrincipal}
   * field so the store only returns that principal's jobs. Use {@link JobFilter#toBuilder()} to
   * preserve all other filter criteria:
   *
   * <pre>{@code
   * return filter.toBuilder().callerPrincipal(callerPrincipal).build();
   * }</pre>
   *
   * <p>Admin or support roles should return the filter unchanged to preserve cross-tenant
   * visibility.
   *
   * <p><strong>Principal precedence:</strong> the security-context principal ({@code
   * callerPrincipal} parameter) always takes precedence over any {@code callerPrincipal} already
   * present in {@code filter}. Implementations MUST use the parameter, not the field, when deciding
   * what the current caller is allowed to see.
   *
   * <p>The default implementation returns the filter unchanged (permit-all semantics).
   *
   * @param filter the original filter supplied by the caller; never {@code null}
   * @param callerPrincipal the current principal; {@code null} if no security context is active
   * @return the filter to use for the store query; must not be {@code null}
   */
  default JobFilter filterForPrincipal(JobFilter filter, String callerPrincipal) {
    return filter;
  }
}
