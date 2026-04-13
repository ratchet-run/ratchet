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
 * <p>When the RI encounters this annotation on a target method's class or method, it wraps
 * invocation through the configured {@link run.ratchet.spi.ResilienceStrategy}.
 *
 * <h3>Usage</h3>
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
   * @return the protected service name (used for logging and grouping circuit breakers)
   */
  @Nonbinding
  String service() default "";

  /**
   * Specifies the circuit breaker profile to be used for the annotated target.
   *
   * <p>Circuit breaker profiles define pre-configured settings for common use cases, such as
   * thresholds, window sizes, and wait durations. This allows for easier customization and reuse of
   * circuit breaker configurations. If not explicitly set, the default profile {@link
   * CircuitBreakerProfile#DEFAULT} will be applied.
   *
   * @return the selected {@link CircuitBreakerProfile} for circuit breaker protection
   */
  @Nonbinding
  CircuitBreakerProfile profile() default CircuitBreakerProfile.DEFAULT;
}
