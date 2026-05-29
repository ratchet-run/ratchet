package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Set;
import org.jboss.logging.Logger;
import run.ratchet.api.DoNotRetry;

/**
 * Determines whether a failed job should be retried based on its exception type. Checks a built-in
 * list of well-known permanent-failure exceptions and the {@link DoNotRetry} annotation. The full
 * cause chain is examined, so wrapping a non-retryable exception does not hide it.
 */
@ApplicationScoped
public class DoNotRetryPolicy {

  private static final Logger log = Logger.getLogger(DoNotRetryPolicy.class);

  // IllegalStateException is intentionally excluded: CDI and JPA throw it for transient conditions
  // (e.g. EntityManager already closed) that may resolve on retry. Use @DoNotRetry for business
  // state errors.
  private static final Set<String> DO_NOT_RETRY_EXCEPTIONS =
      Set.of(
          "java.lang.IllegalArgumentException",
          "java.lang.NullPointerException",
          "java.lang.SecurityException",
          "jakarta.security.enterprise.AuthenticationException");

  public boolean shouldNotRetry(Throwable exception) {
    if (exception == null) {
      return false;
    }

    if (isDoNotRetryException(exception)) {
      log.infof("Exception %s marked as do-not-retry", exception.getClass().getName());
      return true;
    }

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

  private boolean isDoNotRetryException(Throwable exception) {
    String className = exception.getClass().getName();
    if (DO_NOT_RETRY_EXCEPTIONS.contains(className)) {
      return true;
    }

    // Support both the API annotation and the deprecated RI alias during migration.
    return exception.getClass().isAnnotationPresent(DoNotRetry.class);
  }
}
