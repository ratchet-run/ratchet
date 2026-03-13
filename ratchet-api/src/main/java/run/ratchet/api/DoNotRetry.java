package run.ratchet.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an exception type as permanently non-retryable.
 *
 * <p>When a job fails with an exception annotated with {@code @DoNotRetry}, the scheduler skips
 * further retry attempts and moves the job directly to the dead letter flow. Apply this to
 * exceptions that represent permanent business or validation failures rather than transient
 * infrastructure errors.
 *
 * @see run.ratchet.ri.core.DoNotRetryPolicy
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DoNotRetry {

  /**
   * Optional human-readable explanation for why retries should be skipped.
   *
   * @return the operator-facing reason
   */
  String value() default "";
}
