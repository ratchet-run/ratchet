package run.ratchet.ri.core;

import run.ratchet.store.entity.JobPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Consolidates validation-related dependencies for job execution.
 *
 * <p>This validator bundles security validation and retry policy decisions into a single service to
 * reduce dependency coupling in {@link JobTask}. It provides a unified interface for all
 * pre-execution validation and exception handling decisions.
 *
 * @see JobTask
 * @see PostExecutionHandler
 * @see ExecutionObserver
 */
@ApplicationScoped
public class PreExecutionValidator {

  private final SecurityValidator securityValidator;
  private final DoNotRetryPolicy doNotRetryPolicy;

  // Required by CDI proxy
  protected PreExecutionValidator() {
    this.securityValidator = null;
    this.doNotRetryPolicy = null;
  }

  @Inject
  public PreExecutionValidator(
      SecurityValidator securityValidator, DoNotRetryPolicy doNotRetryPolicy) {
    this.securityValidator = securityValidator;
    this.doNotRetryPolicy = doNotRetryPolicy;
  }

  /**
   * Validates a job payload for security constraints before execution.
   *
   * <p>Checks that the target class and method are allowed to be invoked through the scheduler
   * framework (e.g., whitelist checks, method accessibility).
   *
   * @param payload the job payload to validate
   * @throws SecurityException if the payload fails security validation
   * @throws ClassNotFoundException if the target class cannot be found
   * @throws NoSuchMethodException if the target method cannot be found
   */
  public void validateSecurity(JobPayload payload)
      throws ClassNotFoundException, NoSuchMethodException {
    securityValidator.validate(payload);
  }

  /**
   * Determines if an exception should NOT trigger a retry attempt.
   *
   * <p>Some exceptions indicate permanent failures that would be futile to retry, such as
   * validation errors, authorization failures, or configuration issues.
   *
   * @param ex the exception to evaluate
   * @return true if the exception should NOT be retried, false if retry is allowed
   */
  public boolean shouldNotRetry(Throwable ex) {
    return doNotRetryPolicy.shouldNotRetry(ex);
  }

  /**
   * Strategy interface for validating job payloads before execution. Implementations may enforce
   * class whitelists, method visibility checks, or other security constraints.
   */
  @FunctionalInterface
  public interface SecurityValidator {
    void validate(JobPayload payload) throws ClassNotFoundException, NoSuchMethodException;
  }

  /**
   * Strategy interface for determining which exceptions should not trigger retries. Implementations
   * classify exceptions as permanent failures vs. transient failures.
   */
  @FunctionalInterface
  public interface DoNotRetryPolicy {
    boolean shouldNotRetry(Throwable ex);
  }
}
