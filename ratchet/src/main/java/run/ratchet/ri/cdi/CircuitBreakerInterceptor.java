/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.cdi;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import java.lang.reflect.Method;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfigProvider;

/**
 * CDI interceptor that wraps methods annotated with {@link CircuitBreakerProtected} in circuit
 * breaker protection.
 *
 * <p>The interceptor resolves the circuit breaker instance from the {@link CircuitBreakerRegistry}
 * using the service name and profile from the annotation. If the circuit is OPEN, the invocation
 * fails immediately with a {@link run.ratchet.api.exception.CircuitBreakerOpenException}.
 *
 * <p>Service name defaults to {@code ClassName.methodName} if not specified in the annotation.
 *
 * @apiNote Internal RI implementation. The class is public because the CDI interceptor SPI requires
 *     it, but applications must not reference this type directly. Use {@link
 *     CircuitBreakerProtected} to opt methods or classes into the interception. Not part of the
 *     supported API surface.
 */
@Interceptor
@CircuitBreakerProtected
// Run before @Transactional (Jakarta Transactions interceptor priority =
// PLATFORM_BEFORE + 200 = 200). PLATFORM_BEFORE + 100 = 100 fires first, so an OPEN
// circuit short-circuits with CircuitBreakerOpenException before a transaction is opened
// and a connection is borrowed.
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 100)
public class CircuitBreakerInterceptor {

  private final CircuitBreakerRegistry registry;
  private final CircuitBreakerConfigProvider configProvider;

  protected CircuitBreakerInterceptor() {
    this.registry = null;
    this.configProvider = null;
  }

  @Inject
  public CircuitBreakerInterceptor(
      CircuitBreakerRegistry registry, CircuitBreakerConfigProvider configProvider) {
    this.registry = registry;
    this.configProvider = configProvider;
  }

  /** Wraps the intercepted method in circuit breaker protection. */
  @AroundInvoke
  public Object intercept(InvocationContext ctx) throws Exception {
    ResolvedCircuitBreakerAnnotation resolved = resolveAnnotation(ctx);
    if (resolved == null) {
      return ctx.proceed();
    }

    String serviceName = resolveServiceName(resolved.annotation(), resolved.method());
    CircuitBreakerProfile profile = resolved.annotation().profile();

    if (!configProvider.isEnabled()) {
      return ctx.proceed();
    }

    CircuitBreaker breaker = registry.getBreaker(serviceName, profile);
    return breaker.execute(ctx::proceed);
  }

  private ResolvedCircuitBreakerAnnotation resolveAnnotation(InvocationContext ctx) {
    Method method = ctx.getMethod();
    // Method-level annotation takes precedence over class-level
    CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
    if (annotation == null) {
      annotation = method.getDeclaringClass().getAnnotation(CircuitBreakerProtected.class);
    }
    if (annotation != null) {
      return new ResolvedCircuitBreakerAnnotation(annotation, method);
    }

    Object target = ctx.getTarget();
    if (target == null) {
      return null;
    }
    Class<?> targetClass = target.getClass();
    Method targetMethod = method;
    try {
      targetMethod = targetClass.getMethod(method.getName(), method.getParameterTypes());
      annotation = targetMethod.getAnnotation(CircuitBreakerProtected.class);
    } catch (NoSuchMethodException ignored) {
      annotation = null;
    }
    if (annotation == null) {
      annotation = targetClass.getAnnotation(CircuitBreakerProtected.class);
    }
    return annotation != null
        ? new ResolvedCircuitBreakerAnnotation(annotation, targetMethod)
        : null;
  }

  private String resolveServiceName(CircuitBreakerProtected annotation, Method method) {
    String service = annotation.service();
    if (service == null || service.isEmpty()) {
      return method.getDeclaringClass().getSimpleName() + "." + method.getName();
    }
    return service;
  }

  private record ResolvedCircuitBreakerAnnotation(
      CircuitBreakerProtected annotation, Method method) {}
}
