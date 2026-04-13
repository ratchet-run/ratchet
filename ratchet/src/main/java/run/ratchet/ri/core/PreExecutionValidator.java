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

  public void validateSecurity(JobPayload payload) throws NoSuchMethodException {
    securityValidator.validate(payload);
  }

  public boolean shouldNotRetry(Throwable ex) {
    return doNotRetryPolicy.shouldNotRetry(ex);
  }

  /**
   * Strategy interface for validating job payloads before execution. Implementations may enforce
   * class whitelists, method visibility checks, or other security constraints.
   */
  @FunctionalInterface
  public interface SecurityValidator {
    void validate(JobPayload payload) throws NoSuchMethodException;
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
