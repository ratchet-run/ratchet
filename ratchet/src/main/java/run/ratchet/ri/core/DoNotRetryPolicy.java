package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Determines whether a failed job should be retried based on its exception type. Checks a built-in
 * list of well-known permanent-failure exceptions and the {@link
 * run.ratchet.api.DoNotRetry} annotation. The full cause chain is examined, so wrapping a
 * non-retryable exception does not hide it.
 */
@ApplicationScoped
public class DoNotRetryPolicy {

  private static final Logger log = Logger.getLogger(DoNotRetryPolicy.class);

  /** Well-known exception class names that indicate permanent, non-retryable failures. */
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
