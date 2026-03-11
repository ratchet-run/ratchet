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

  /** The service name for circuit breaker identification. Defaults to ClassName.methodName. */
  @Nonbinding
  String service() default "";

  /** The circuit breaker profile to use. */
  @Nonbinding
  CircuitBreakerProfile profile() default CircuitBreakerProfile.DEFAULT;
}
