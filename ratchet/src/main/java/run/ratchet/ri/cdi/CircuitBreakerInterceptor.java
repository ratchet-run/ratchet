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

  /**
   * Registry for managing circuit breaker instances used within the interceptor.
   *
   * <p>The {@code registry} is a central component for retrieving and managing circuit breakers
   * tied to specific services and profiles. It allows for dynamic and efficient protection of
   * service invocations, preventing cascading failures in the presence of faults.
   *
   * <p>The circuit breakers are resolved based on the service name and profile specified in the
   * {@link CircuitBreakerProtected} annotation. This registry ensures that the appropriate circuit
   * breaker is applied during method interception.
   *
   * <p>The {@code registry} is injected into the interceptor to enable dependency inversion and
   * facilitate the loose coupling of components in the resilience layer.
   */
  private final CircuitBreakerRegistry registry;

  // Required by CDI proxy
  protected CircuitBreakerInterceptor() {
    this.registry = null;
  }

  @Inject
  public CircuitBreakerInterceptor(CircuitBreakerRegistry registry) {
    this.registry = registry;
  }

  /**
   * Intercepts method invocations to enforce circuit breaker protection for methods annotated with
   * {@link CircuitBreakerProtected}. This method uses a circuit breaker from the {@link
   * CircuitBreakerRegistry} to guard against failures in method execution.
   *
   * @param ctx the {@link InvocationContext} providing information about the intercepted method
   *     invocation, including the method, parameters, and target object.
   * @return the result of the method invocation, either the original method's return value or an
   *     exception if circuit protection is triggered.
   * @throws IllegalStateException if the {@link CircuitBreakerRegistry} is not initialized.
   * @throws Exception if the method execution, protected by the circuit breaker, throws an
   *     exception.
   */
  @AroundInvoke
  public Object intercept(InvocationContext ctx) throws Exception {
    if (registry == null) {
      throw new IllegalStateException("Circuit breaker registry not initialized");
    }

    Method method = ctx.getMethod();
    CircuitBreakerProtected annotation = resolveAnnotation(method);

    String serviceName = resolveServiceName(annotation, method);
    CircuitBreakerProfile profile = annotation.profile();

    CircuitBreaker breaker = registry.getBreaker(serviceName, profile);
    return breaker.execute(ctx::proceed);
  }

  /**
   * Resolves the {@link CircuitBreakerProtected} annotation from the provided method or its
   * declaring class.
   *
   * <p>This method first checks if the given method is annotated with {@code
   * CircuitBreakerProtected}. If no method-level annotation is found, it checks for the annotation
   * on the declaring class of the method. Method-level annotations take precedence over class-level
   * annotations.
   *
   * @param method the {@link Method} object to inspect for the {@code CircuitBreakerProtected}
   *     annotation
   * @return the {@link CircuitBreakerProtected} annotation found on the method or its declaring
   *     class, or {@code null} if no such annotation is present
   */
  private CircuitBreakerProtected resolveAnnotation(Method method) {
    // Method-level annotation takes precedence over class-level
    CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
    if (annotation == null) {
      annotation = method.getDeclaringClass().getAnnotation(CircuitBreakerProtected.class);
    }
    return annotation;
  }

  /**
   * Resolves the service name associated with the {@link CircuitBreakerProtected} annotation. If
   * the `service` attribute of the annotation is not explicitly set or is empty, the method returns
   * a default service name derived from the declaring class and method name.
   *
   * @param annotation the {@link CircuitBreakerProtected} annotation from which to retrieve the
   *     service name
   * @param method the {@link Method} object for which the service name is being resolved
   * @return the resolved service name as a {@code String}. If the `service` attribute is empty or
   *     null, the service name is constructed as the combination of the declaring class's simple
   *     name and the method name in the format "ClassName.methodName".
   */
  private String resolveServiceName(CircuitBreakerProtected annotation, Method method) {
    String service = annotation.service();
    if (service == null || service.isEmpty()) {
      return method.getDeclaringClass().getSimpleName() + "." + method.getName();
    }
    return service;
  }
}
