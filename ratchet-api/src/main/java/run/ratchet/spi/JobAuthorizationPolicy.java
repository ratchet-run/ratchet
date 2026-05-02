package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.exception.JobAuthorizationException;
import java.util.UUID;

/**
 * SPI for authorizing job operations.
 *
 * <p>The reference implementation ({@code PermitAllJobAuthorizationPolicy}) allows every operation.
 * Integrators replace it with a CDI {@code @Alternative @Priority(APPLICATION)} bean to enforce
 * site-specific rules.
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
 * <p>{@code JobSchedulerService.cancelRecurringJobsByTag} and {@code
 * cancelRecurringJobByBusinessKey} are not subject to per-job authorization checks. Use {@code
 * cancelJob(UUID)} for authorization-gated cancellation.
 */
@Incubating
public interface JobAuthorizationPolicy {

  /**
   * Called at direct API job submission, within the {@code REQUIRED} transaction, after the caller
   * principal has been captured but before the job is persisted.
   *
   * <p>Also called for chain steps and workflow branches at their creation points; batch streaming
   * children are excluded (see class-level note on bulk fan-out).
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
}
