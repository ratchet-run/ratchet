package run.ratchet.ri.cdi;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import java.lang.reflect.Method;

/**
 * CDI interceptor that wraps methods annotated with {@link CircuitBreakerProtected} in circuit
 * breaker protection.
 *
 * <p>The interceptor resolves the circuit breaker instance from the {@link CircuitBreakerRegistry}
 * using the service name and profile from the annotation. If the circuit is OPEN, the invocation
 * fails immediately with a {@link run.ratchet.ri.resilience.ServiceUnavailableException}.
 *
 * <p>Service name defaults to {@code ClassName.methodName} if not specified in the annotation.
 */
@Interceptor
@CircuitBreakerProtected
@Priority(Interceptor.Priority.LIBRARY_BEFORE + 100)
public class CircuitBreakerInterceptor {

  private final CircuitBreakerRegistry registry;

  // Required by CDI proxy
  protected CircuitBreakerInterceptor() {
    this.registry = null;
  }

  @Inject
  public CircuitBreakerInterceptor(CircuitBreakerRegistry registry) {
    this.registry = registry;
  }

  /** Wraps the intercepted method in circuit breaker protection. */
  @AroundInvoke
  public Object intercept(InvocationContext ctx) throws Exception {
    Method method = ctx.getMethod();
    CircuitBreakerProtected annotation = resolveAnnotation(method);

    String serviceName = resolveServiceName(annotation, method);
    CircuitBreakerProfile profile = annotation.profile();

    CircuitBreaker breaker = registry.getBreaker(serviceName, profile);
    return breaker.execute(ctx::proceed);
  }

  private CircuitBreakerProtected resolveAnnotation(Method method) {
    // Method-level annotation takes precedence over class-level
    CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
    if (annotation == null) {
      annotation = method.getDeclaringClass().getAnnotation(CircuitBreakerProtected.class);
    }
    return annotation;
  }

  private String resolveServiceName(CircuitBreakerProtected annotation, Method method) {
    String service = annotation.service();
    if (service == null || service.isEmpty()) {
      return method.getDeclaringClass().getSimpleName() + "." + method.getName();
    }
    return service;
  }
}
