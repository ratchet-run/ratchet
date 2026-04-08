package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Policy that determines whether a job should be retried based on the exception type.
 *
 * <p>This policy is a critical component of the scheduler's error handling strategy. It examines
 * exceptions thrown by job execution and determines whether the job should be retried or
 * immediately moved to the Dead Letter Queue (DLQ).
 *
 * <p>Retrying jobs that are destined to fail wastes system resources and delays processing of other
 * jobs. This policy identifies "permanent" failures that will never succeed regardless of how many
 * times they are retried:
 *
 * <ul>
 *   <li><b>Validation errors:</b> IllegalArgumentException, IllegalStateException,
 *       NullPointerException - the input is bad and won't change
 *   <li><b>Security errors:</b> SecurityException, AuthenticationException - the caller lacks
 *       permission and retrying won't grant it
 *   <li><b>Business logic errors:</b> Custom exceptions that indicate invalid state
 * </ul>
 *
 * <p>The policy uses two mechanisms to identify non-retryable exceptions:
 *
 * <ol>
 *   <li><b>Built-in list:</b> Well-known JDK and Jakarta EE exception classes
 *   <li><b>Annotation:</b> Custom exceptions annotated with {@link
 *       run.ratchet.api.DoNotRetry}
 * </ol>
 *
 * <p>The entire exception cause chain is examined, so wrapping a non-retryable exception in another
 * exception will still prevent retries.
 *
 * @see run.ratchet.api.DoNotRetry for marking custom exceptions as non-retryable
 */
@ApplicationScoped
public class DoNotRetryPolicy {

  private static final Logger log = Logger.getLogger(DoNotRetryPolicy.class);

  /**
   * Set of fully qualified exception class names that should never be retried.
   *
   * <p>These are well-known JDK and framework exception classes that indicate permanent failures.
   * Using fully qualified class names (rather than Class objects) allows this set to include
   * exceptions that may not be on the classpath in all deployments.
   *
   * <p>This list is intentionally conservative - only exceptions that are clearly non-recoverable
   * are included. When in doubt, exceptions default to being retryable, which is the safer
   * behavior.
   */
  private static final Set<String> DO_NOT_RETRY_EXCEPTIONS =
      Set.of(
          // Validation errors - won't succeed on retry
          "java.lang.IllegalArgumentException",
          // IllegalStateException intentionally excluded: CDI and JPA throw it for transient
          // container lifecycle conditions (e.g. EntityManager already closed, transaction
          // already active) that may resolve on retry. Jobs that fail with bad business state
          // should annotate their custom exception with @DoNotRetry instead.
          "java.lang.NullPointerException",

          // Security errors - authentication/authorization failures
          "java.lang.SecurityException",
          "jakarta.security.enterprise.AuthenticationException",
          "jakarta.security.enterprise.AuthenticationStatus");

  /**
   * Checks if the given exception should not be retried.
   *
   * <p>This method checks:
   *
   * <ol>
   *   <li>If the exception class name is in the do-not-retry list
   *   <li>If any cause of the exception is in the do-not-retry list
   *   <li>If the exception or its cause is annotated with {@code @DoNotRetry}
   * </ol>
   *
   * @param exception the exception to check
   * @return true if the exception should not be retried, false otherwise
   */
  public boolean shouldNotRetry(Throwable exception) {
    if (exception == null) {
      return false;
    }

    // Check the exception itself
    if (isDoNotRetryException(exception)) {
      log.infof("Exception %s marked as do-not-retry", exception.getClass().getName());
      return true;
    }

    // Check all causes recursively
    Throwable cause = exception.getCause();
    while (cause != null && cause != exception) {
      if (isDoNotRetryException(cause)) {
        log.infof("Exception cause %s marked as do-not-retry", cause.getClass().getName());
        return true;
      }
      cause = cause.getCause();
    }

    return false;
  }

  /**
   * Checks if a specific exception should not be retried.
   *
   * <p>This method performs two checks:
   *
   * <ol>
   *   <li>Whether the exception's fully qualified class name is in the {@link
   *       #DO_NOT_RETRY_EXCEPTIONS} set
   *   <li>Whether the exception class is annotated with {@link run.ratchet.api.DoNotRetry}
   * </ol>
   *
   * <p>This method only checks the given exception instance, not its cause chain. The cause chain
   * traversal is handled by {@link #shouldNotRetry(Throwable)}.
   *
   * @param exception the specific exception instance to check (not the cause chain)
   * @return true if this specific exception type should not be retried
   */
  @SuppressWarnings("removal")
  private boolean isDoNotRetryException(Throwable exception) {
    // Check if exception class is in the do-not-retry list
    String className = exception.getClass().getName();
    if (DO_NOT_RETRY_EXCEPTIONS.contains(className)) {
      return true;
    }

    // Support both the API annotation and the deprecated RI alias during migration.
    return exception.getClass().isAnnotationPresent(run.ratchet.api.DoNotRetry.class)
        || exception.getClass().isAnnotationPresent(run.ratchet.ri.core.DoNotRetry.class);
  }
}
