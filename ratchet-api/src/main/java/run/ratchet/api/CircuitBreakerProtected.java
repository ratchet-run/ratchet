package run.ratchet.api;

import jakarta.enterprise.util.Nonbinding;
import jakarta.interceptor.InterceptorBinding;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a job method or class for circuit breaker protection.
 *
 * <p>When the RI encounters this annotation on a target method's class or method, its CDI
 * interceptor wraps the invocation in the built-in circuit breaker selected by {@link #service()}
 * and {@link #profile()}.
 *
 * <p>When the breaker is open, the RI rejects the invocation before the target method runs. The
 * rejection is surfaced as the RI resilience exception used by the configured interceptor; portable
 * callers should treat it as a runtime circuit-open failure rather than as an exception thrown by
 * the annotated method body.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * @CircuitBreakerProtected(service = "payment-gateway", profile = CircuitBreakerProfile.EXTERNAL_API)
 * public PaymentResult processPayment(PaymentRequest request) {
 *     return gateway.charge(request);
 * }
 * }</pre>
 */
@Incubating
@InterceptorBinding
@Inherited
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CircuitBreakerProtected {

  /**
   * Returns the circuit breaker service key. When empty, the RI derives the key from the
   * fully-qualified method name.
   *
   * @return circuit breaker service key, or empty for the derived method key
   */
  @Nonbinding
  String service() default "";

  /**
   * Returns the circuit breaker profile used for this method or class.
   *
   * @return circuit breaker profile
   */
  @Nonbinding
  CircuitBreakerProfile profile() default CircuitBreakerProfile.DEFAULT;
}
