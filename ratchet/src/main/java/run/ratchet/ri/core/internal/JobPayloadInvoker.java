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
package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.objectweb.asm.Type;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.payload.ArgumentCoercion;
import run.ratchet.ri.payload.ArgumentMaterializer;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

/**
 * Resolves and invokes persisted job payloads without retaining application classloaders.
 *
 * <p>Reflection results are stored in {@link ClassValue} caches. The JVM associates each entry with
 * the exact {@link Class}, so two deployments defining the same binary name cannot share a cached
 * {@link Method}, and unloading a deployment also releases its cache entries. The class policy is
 * checked before every load, including cache hits, so a runtime policy change cannot be bypassed by
 * an earlier allowed invocation.
 */
@ApplicationScoped
public class JobPayloadInvoker {

  private final ClassValue<ConcurrentMap<MethodKey, Method>> methodCache =
      new ClassValue<>() {
        @Override
        protected ConcurrentMap<MethodKey, Method> computeValue(Class<?> type) {
          return new ConcurrentHashMap<>();
        }
      };

  private final ClassValue<ConcurrentMap<MethodKey, String>> serviceNameCache =
      new ClassValue<>() {
        @Override
        protected ConcurrentMap<MethodKey, String> computeValue(Class<?> type) {
          return new ConcurrentHashMap<>();
        }
      };

  private final BeanResolver beanResolver;
  private final ClassPolicy classPolicy;

  protected JobPayloadInvoker() {
    this.beanResolver = null;
    this.classPolicy = null;
  }

  @Inject
  public JobPayloadInvoker(BeanResolver beanResolver, ClassPolicy classPolicy) {
    this.beanResolver = beanResolver;
    this.classPolicy = classPolicy;
  }

  /** Restores persisted arguments while applying this deployment's class policy. */
  public JobPayload materializeArguments(JobPayload payload, PayloadSerializer serializer) {
    return ArgumentMaterializer.materialize(payload, serializer, classPolicy);
  }

  /** Resolves and invokes one payload against its static target or CDI bean. */
  public Object invoke(JobPayload payload) throws Exception {
    Class<?> targetClass = loadAllowedClass(payload.target());
    Method method = resolveMethod(targetClass, payload);
    List<Object> args = payload.args() != null ? payload.args() : List.of();
    if (payload.isStatic()) {
      return invokeTargetMethod(method, null, args);
    }
    try (BeanResolver.ManagedBeanHandle<?> handle = resolveBean(targetClass, payload)) {
      return invokeTargetMethod(method, handle.get(), args);
    }
  }

  /**
   * Returns the circuit-breaker service name for a payload, falling back to class and method when
   * its target cannot be resolved.
   */
  public String serviceName(JobPayload payload) {
    String fallback = simpleClassName(payload.target()) + "." + payload.method();
    try {
      Class<?> targetClass = loadAllowedClass(payload.target());
      MethodKey key = MethodKey.from(payload);
      String cached = serviceNameCache.get(targetClass).get(key);
      if (cached != null) {
        return cached;
      }

      Method method = resolveMethod(targetClass, payload);
      CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
      if (annotation == null) {
        annotation = targetClass.getAnnotation(CircuitBreakerProtected.class);
      }
      String resolved =
          annotation != null && annotation.service() != null && !annotation.service().isBlank()
              ? annotation.service()
              : targetClass.getSimpleName() + "." + method.getName();
      String existing = serviceNameCache.get(targetClass).putIfAbsent(key, resolved);
      return existing != null ? existing : resolved;
    } catch (Exception e) {
      return fallback;
    }
  }

  private Class<?> loadAllowedClass(String className) throws ClassNotFoundException {
    if (className == null || className.isEmpty()) {
      throw new SecurityException("Class name cannot be null or empty");
    }
    if (classPolicy != null
        && !JobPayloadFactory.isRecurringDispatchShim(className)
        && !classPolicy.isAllowed(className)) {
      throw new SecurityException("Class " + className + " is not allowed for job execution.");
    }
    return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
  }

  private Method resolveMethod(Class<?> targetClass, JobPayload payload)
      throws NoSuchMethodException {
    MethodKey key = MethodKey.from(payload);
    ConcurrentMap<MethodKey, Method> methods = methodCache.get(targetClass);
    Method cached = methods.get(key);
    if (cached != null) {
      return cached;
    }

    for (Method method : targetClass.getMethods()) {
      if (matches(method, key)) {
        Method existing = methods.putIfAbsent(key, method);
        return existing != null ? existing : method;
      }
    }

    for (Method method : targetClass.getDeclaredMethods()) {
      if (matches(method, key)) {
        String visibility =
            Modifier.isPrivate(method.getModifiers())
                ? "private"
                : Modifier.isProtected(method.getModifiers()) ? "protected" : "package-private";
        throw new NoSuchMethodException(
            payload.method()
                + " in "
                + targetClass.getName()
                + " is "
                + visibility
                + " — only public methods can be scheduled as jobs. Change the method visibility"
                + " to public.");
      }
    }

    throw new NoSuchMethodException(
        payload.method() + " with descriptor " + payload.methodDescriptor());
  }

  private BeanResolver.ManagedBeanHandle<?> resolveBean(Class<?> targetClass, JobPayload payload) {
    try {
      return beanResolver.resolveManaged(targetClass);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot resolve bean for instance method "
              + payload.method()
              + " in class "
              + payload.target()
              + ". Ensure the class is a managed bean or use a static method.",
          e);
    }
  }

  private static Object invokeTargetMethod(Method method, Object target, List<Object> args)
      throws Exception {
    try {
      return method.invoke(
          target, ArgumentCoercion.coerce(method.getParameterTypes(), args.toArray()));
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
  }

  private static boolean matches(Method method, MethodKey key) {
    return method.getName().equals(key.method())
        && Type.getMethodDescriptor(method).equals(key.descriptor());
  }

  private static String simpleClassName(String className) {
    int lastDot = className.lastIndexOf('.');
    return lastDot >= 0 ? className.substring(lastDot + 1) : className;
  }

  private record MethodKey(String method, String descriptor) {
    private static MethodKey from(JobPayload payload) {
      return new MethodKey(payload.method(), payload.methodDescriptor());
    }
  }
}
