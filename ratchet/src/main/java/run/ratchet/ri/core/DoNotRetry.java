package run.ratchet.ri.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark exception classes that should not be retried by the job scheduler.
 *
 * <p>When a job throws an exception annotated with {@code @DoNotRetry}, the scheduler will skip
 * retry attempts and move the job directly to the Dead Letter Queue (DLQ). This annotation provides
 * a declarative way to indicate that certain failure modes are permanent and retrying would be
 * futile.
 *
 * <p><b>When to use this annotation:</b>
 *
 * <ul>
 *   <li><b>Validation errors:</b> Input data is invalid and won't change on retry
 *   <li><b>Authorization failures:</b> User lacks permission; retrying won't help
 *   <li><b>Business rule violations:</b> Operation is not allowed in current state
 *   <li><b>Data integrity issues:</b> Referenced data doesn't exist or is corrupted
 *   <li><b>Configuration errors:</b> Missing or invalid configuration
 * </ul>
 *
 * <p><b>When NOT to use this annotation:</b>
 *
 * <ul>
 *   <li>Network timeouts or transient connection failures
 *   <li>Rate limiting responses (HTTP 429)
 *   <li>Database deadlocks or lock timeouts
 *   <li>Temporary resource exhaustion
 * </ul>
 *
 * <p>
 *
 * <h3>Usage Example:</h3>
 *
 * <pre>{@code
 * @DoNotRetry("Invalid input cannot be fixed by retrying")
 * public class ValidationException extends RuntimeException {
 *     // This exception will not trigger retries
 * }
 *
 * // In your job code:
 * public void processOrder(String orderId) {
 *     if (orderId == null) {
 *         throw new ValidationException("Order ID cannot be null");
 *         // Job will move directly to DLQ, no retries
 *     }
 *     // ... process order
 * }
 * }</pre>
 *
 * <p>The annotation is checked by {@link DoNotRetryPolicy} which examines both the thrown exception
 * and its cause chain. If any exception in the chain is annotated with {@code @DoNotRetry}, the job
 * will not be retried.
 *
 * @see DoNotRetryPolicy for the policy implementation that checks this annotation
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DoNotRetry {

  /**
   * Optional reason for why this exception should not be retried.
   *
   * <p>This value is used for documentation and logging purposes. When a job fails with an
   * exception marked with this annotation, the reason can be included in log messages and DLQ
   * entries to help with debugging and operational monitoring.
   *
   * <p>Example: {@code @DoNotRetry("User authentication failed - credentials invalid")}
   *
   * @return a human-readable explanation of why retries are inappropriate
   */
  String value() default "";
}
