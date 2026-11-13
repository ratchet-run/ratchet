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
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.objectweb.asm.Type;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobContext;
import run.ratchet.api.TransactionalJob;
import run.ratchet.ri.payload.ArgumentCoercion;
import run.ratchet.ri.payload.ArgumentMaterializer;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.CircuitBreakerExceptionFilter;
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

  private final ClassValue<ConcurrentMap<MethodKey, ResilienceTarget>> resilienceTargetCache =
      new ClassValue<>() {
        @Override
        protected ConcurrentMap<MethodKey, ResilienceTarget> computeValue(Class<?> type) {
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
  public JobPayload materializeArguments(JobPayload payload, PayloadSerializer serializer)
      throws ClassNotFoundException, NoSuchMethodException {
    Class<?> targetClass = loadAllowedClass(payload.target());
    Method method = resolveMethod(targetClass, payload);
    return ArgumentMaterializer.materialize(
        payload, method.getGenericParameterTypes(), serializer, classPolicy);
  }

  /** Resolves and invokes one payload against its static target or CDI bean. */
  public Object invoke(JobPayload payload) throws Exception {
    Class<?> targetClass = loadAllowedClass(payload.target());
    Method method = resolveMethod(targetClass, payload);
    List<Object> args = payload.args() != null ? payload.args() : List.of();
    if (payload.isStatic()) return invokeTargetMethod(method, null, args);
    try (BeanResolver.ManagedBean handle = resolveBean(targetClass, payload)) {
      Object target = handle.instance();
      return invokeTargetMethod(ManagedInvocation.exposedMethod(method, target), target, args);
    }
  }

  /**
   * Returns the circuit-breaker service name and exception filter, falling back to class and method
   * when its target cannot be resolved.
   */
  public ResilienceTarget resilienceTarget(JobPayload payload) {
    if (JobPayloadFactory.isRecurringDispatchShim(payload)) {
      ResilienceTarget dispatcher = directResilienceTarget(payload);
      try {
        ResilienceTarget body = directResilienceTarget(recurringBody(payload));
        return new ResilienceTarget(
            dispatcher.serviceName(), dispatcher.exceptionFilter(), body.transactional());
      } catch (RuntimeException ignored) {
        return dispatcher;
      }
    }
    return directResilienceTarget(payload);
  }

  private ResilienceTarget directResilienceTarget(JobPayload payload) {
    String fallback = simpleClassName(payload.target()) + "." + payload.method();
    try {
      Class<?> targetClass = loadAllowedClass(payload.target());
      MethodKey key = MethodKey.from(payload);
      ResilienceTarget cached = resilienceTargetCache.get(targetClass).get(key);
      if (cached != null) {
        return cached;
      }

      Method method = resolveMethod(targetClass, payload);
      CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
      if (annotation == null) {
        annotation = targetClass.getAnnotation(CircuitBreakerProtected.class);
      }
      String serviceName =
          annotation != null && annotation.service() != null && !annotation.service().isBlank()
              ? annotation.service()
              : targetClass.getSimpleName() + "." + method.getName();
      CircuitBreakerExceptionFilter filter =
          annotation == null
              ? CircuitBreakerExceptionFilter.RECORD_ALL
              : CircuitBreakerExceptionFilter.of(
                  annotation.recordExceptions(), annotation.ignoreExceptions());
      ResilienceTarget resolved =
          new ResilienceTarget(
              serviceName,
              filter,
              method.isAnnotationPresent(TransactionalJob.class)
                  || targetClass.isAnnotationPresent(TransactionalJob.class));
      ResilienceTarget existing = resilienceTargetCache.get(targetClass).putIfAbsent(key, resolved);
      return existing != null ? existing : resolved;
    } catch (Exception e) {
      return new ResilienceTarget(fallback, CircuitBreakerExceptionFilter.RECORD_ALL, false);
    }
  }

  /**
   * Resolved resilience metadata for a job target.
   *
   * @param serviceName the protected service key
   * @param exceptionFilter exception accounting rules from the target annotation
   * @param transactional whether the job body joins its success completion transaction
   */
  public record ResilienceTarget(
      String serviceName, CircuitBreakerExceptionFilter exceptionFilter, boolean transactional) {}

  /** Checks transactional capability and direct transaction escapes before persisting a job. */
  public void validateTransactionalSubmission(JobPayload payload, boolean capabilityAvailable) {
    if (!resilienceTarget(payload).transactional()) return;
    if (!capabilityAvailable) {
      throw new UnsupportedOperationException(
          "@TransactionalJob requires a store advertising TransactionalCompletionStore");
    }
    try {
      if (JobPayloadFactory.isRecurringDispatchShim(payload)) payload = recurringBody(payload);
      Class<?> targetClass = loadAllowedClass(payload.target());
      Method method = resolveMethod(targetClass, payload);
      rejectTransactionEscape(method);
      rejectTransactionEscape(targetClass);
    } catch (ReflectiveOperationException ignored) {
      // Best effort: execution retains the usual target-resolution diagnostics.
    }
  }

  private static JobPayload recurringBody(JobPayload dispatcher) {
    List<Object> args = dispatcher.args();
    String descriptor =
        Boolean.TRUE.equals(args.get(2))
            ? Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(JobContext.class))
            : "()V";
    return new JobPayload(
        (String) args.get(0), (String) args.get(1), descriptor, false, List.of(), null);
  }

  /**
   * Rejects transaction escapes throughout composed annotations. A meta-annotation declaring
   * REQUIRES_NEW is rejected even when an aliased attribute resets it to REQUIRED.
   */
  private static void rejectTransactionEscape(AnnotatedElement target) {
    for (Annotation annotation : target.getAnnotations()) {
      inspectTransactionAnnotation(
          annotation, annotation.annotationType().getName(), new HashSet<>());
    }
  }

  private static boolean inspectTransactionAnnotation(
      Annotation annotation, String writtenName, Set<Class<?>> visited) {
    Class<? extends Annotation> type = annotation.annotationType();
    if (type.getName().startsWith("java.lang.annotation.") || !visited.add(type)) return false;
    boolean transactional =
        List.of(
                "jakarta.transaction.Transactional",
                "org.springframework.transaction.annotation.Transactional",
                "jakarta.ejb.TransactionAttribute",
                "io.micronaut.transaction.annotation.Transactional")
            .contains(type.getName());
    for (Annotation meta : type.getAnnotations()) {
      transactional |= inspectTransactionAnnotation(meta, writtenName, visited);
    }
    if (transactional) {
      for (Method attribute : type.getDeclaredMethods()) {
        if (attribute.getParameterCount() != 0) continue;
        // A package-private composed annotation from another package is otherwise unreadable.
        attribute.trySetAccessible();
        Object propagation;
        try {
          propagation = attribute.invoke(annotation);
        } catch (ReflectiveOperationException ignored) {
          continue;
        }
        if (propagation instanceof Enum<?> value
            && List.of(
                    "org.springframework.transaction.annotation.Propagation",
                    "jakarta.transaction.Transactional$TxType",
                    "jakarta.ejb.TransactionAttributeType",
                    "io.micronaut.transaction.TransactionDefinition$Propagation")
                .contains(value.getDeclaringClass().getName())
            && List.of("REQUIRES_NEW", "NOT_SUPPORTED", "NEVER").contains(value.name())) {
          throw new IllegalArgumentException(
              "@TransactionalJob cannot use " + writtenName + " with " + value.name());
        }
      }
    }
    visited.remove(type);
    return transactional;
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

  private BeanResolver.ManagedBean resolveBean(Class<?> targetClass, JobPayload payload) {
    try {
      return beanResolver.acquire(targetClass);
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
