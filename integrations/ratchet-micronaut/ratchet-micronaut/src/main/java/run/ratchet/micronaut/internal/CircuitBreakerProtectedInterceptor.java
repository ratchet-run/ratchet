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
package run.ratchet.micronaut.internal;

import io.micronaut.aop.InterceptPhase;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanProvider;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import java.lang.reflect.Method;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerExceptionFilter;

/** Applies the portable circuit breaker binding before any transaction advice. */
@Singleton
@InterceptorBean(CircuitBreakerProtected.class)
public final class CircuitBreakerProtectedInterceptor
    implements MethodInterceptor<Object, Object>, Ordered {
  private final BeanProvider<CircuitBreakerRegistry> registry;
  private final BeanProvider<CircuitBreakerConfigProvider> configProvider;

  /** Keeps policy beans lazy so disabled and store-free applications can still use the advice. */
  public CircuitBreakerProtectedInterceptor(
      BeanProvider<CircuitBreakerRegistry> registry,
      BeanProvider<CircuitBreakerConfigProvider> configProvider) {
    this.registry = registry;
    this.configProvider = configProvider;
  }

  @Override
  public int getOrder() {
    return InterceptPhase.TRANSACTION.getPosition() - 2;
  }

  @Override
  public Object intercept(MethodInvocationContext<Object, Object> context) {
    ResolvedCircuitBreakerAnnotation resolved = resolveAnnotation(context);
    if (resolved == null
        || !configProvider.isPresent()
        || !configProvider.get().isEnabled()
        || !registry.isPresent()) {
      return context.proceed();
    }
    CircuitBreakerProtected annotation = resolved.annotation();
    CircuitBreaker breaker =
        registry
            .get()
            .getBreaker(resolveServiceName(annotation, resolved.method()), annotation.profile());
    CircuitBreakerExceptionFilter filter =
        CircuitBreakerExceptionFilter.of(
            annotation.recordExceptions(), annotation.ignoreExceptions());
    try {
      return breaker.execute(context::proceed, filter);
    } catch (Exception failure) {
      // MethodInterceptor has no checked throws clause. CDI and CircuitBreaker preserve identity,
      // so preserve it here too rather than introducing an integration-specific wrapper.
      return rethrow(failure);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> Object rethrow(Throwable failure) throws E {
    throw (E) failure;
  }

  private ResolvedCircuitBreakerAnnotation resolveAnnotation(
      MethodInvocationContext<Object, Object> ctx) {
    Method method = ctx.getExecutableMethod().getTargetMethod();
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
