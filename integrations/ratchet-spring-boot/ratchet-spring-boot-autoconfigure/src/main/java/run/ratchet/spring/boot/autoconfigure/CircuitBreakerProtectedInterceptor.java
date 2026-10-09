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
package run.ratchet.spring.boot.autoconfigure;

import java.lang.reflect.Method;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.beans.factory.ObjectProvider;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerExceptionFilter;

/** Application-bean advice with the same annotation resolution as the CDI interceptor. */
final class CircuitBreakerProtectedInterceptor implements MethodInterceptor {
  private final ObjectProvider<CircuitBreakerRegistry> registries;
  private final ObjectProvider<CircuitBreakerConfigProvider> configurations;

  CircuitBreakerProtectedInterceptor(
      ObjectProvider<CircuitBreakerRegistry> registries,
      ObjectProvider<CircuitBreakerConfigProvider> configurations) {
    this.registries = registries;
    this.configurations = configurations;
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    ResolvedAnnotation resolved = resolve(invocation);
    if (resolved == null) return invocation.proceed();
    CircuitBreakerConfigProvider configuration = configurations.getIfAvailable();
    if (configuration == null || !configuration.isEnabled()) return invocation.proceed();
    CircuitBreakerRegistry registry = registries.getIfAvailable();
    if (registry == null) return invocation.proceed();
    CircuitBreakerProtected annotation = resolved.annotation();
    String service = annotation.service();
    if (service.isEmpty()) {
      service =
          resolved.method().getDeclaringClass().getSimpleName() + "." + resolved.method().getName();
    }
    CircuitBreaker breaker = registry.getBreaker(service, annotation.profile());
    CircuitBreakerExceptionFilter filter =
        CircuitBreakerExceptionFilter.of(
            annotation.recordExceptions(), annotation.ignoreExceptions());
    return breaker.execute(() -> proceed(invocation), filter);
  }

  private static Object proceed(MethodInvocation invocation) throws Exception {
    try {
      return invocation.proceed();
    } catch (Throwable failure) {
      // Callable permits Exception, whereas AOP Alliance permits Throwable. Preserve identity.
      return rethrow(failure);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> Object rethrow(Throwable failure) throws E {
    throw (E) failure;
  }

  private static ResolvedAnnotation resolve(MethodInvocation invocation) {
    Method method = invocation.getMethod();
    CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
    if (annotation == null) {
      annotation = method.getDeclaringClass().getAnnotation(CircuitBreakerProtected.class);
    }
    if (annotation != null) return new ResolvedAnnotation(annotation, method);
    Object target = invocation.getThis();
    if (target == null) return null;
    Class<?> targetClass = target.getClass();
    Method targetMethod = method;
    try {
      targetMethod = targetClass.getMethod(method.getName(), method.getParameterTypes());
      annotation = targetMethod.getAnnotation(CircuitBreakerProtected.class);
    } catch (NoSuchMethodException ignored) {
      annotation = null;
    }
    if (annotation == null) annotation = targetClass.getAnnotation(CircuitBreakerProtected.class);
    return annotation == null ? null : new ResolvedAnnotation(annotation, targetMethod);
  }

  private record ResolvedAnnotation(CircuitBreakerProtected annotation, Method method) {}
}
