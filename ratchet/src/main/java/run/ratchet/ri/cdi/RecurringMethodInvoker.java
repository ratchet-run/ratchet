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

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import run.ratchet.api.JobContext;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;

/** Invokes @Recurring methods on their CDI beans. */
@ApplicationScoped
public class RecurringMethodInvoker {

  private final ConcurrentMap<MethodCacheKey, Method> methodCache = new ConcurrentHashMap<>();
  private final BeanResolver beanResolver;
  private final ClassPolicy classPolicy;

  protected RecurringMethodInvoker() {
    this.beanResolver = null;
    this.classPolicy = null;
  }

  @Inject
  public RecurringMethodInvoker(BeanResolver beanResolver, ClassPolicy classPolicy) {
    this.beanResolver = beanResolver;
    this.classPolicy = classPolicy;
  }

  @SuppressWarnings("java:S112")
  public void invoke(String beanClassName, String methodName, boolean hasJobContextParam)
      throws Exception {
    if (!classPolicy.isAllowed(beanClassName)) {
      throw new SecurityException(
          "Class " + beanClassName + " is not allowed for recurring job execution.");
    }
    Class<?> beanClass =
        Class.forName(beanClassName, true, Thread.currentThread().getContextClassLoader());
    BeanResolver.ManagedBeanHandle<?> handle;
    try {
      handle = beanResolver.resolveManaged(beanClass);
    } catch (IllegalStateException e) {
      throw new IllegalStateException("No managed bean found for class: " + beanClassName, e);
    }

    try (handle) {
      Object bean = handle.get();
      Method method =
          getOrResolveMethod(beanClass, bean.getClass(), methodName, hasJobContextParam);
      if (hasJobContextParam) {
        JobContext context = JobContext.current();
        invokeMethod(method, bean, context);
      } else {
        invokeMethod(method, bean);
      }
    }
  }

  public void validateBeanResolvable(Class<?> beanClass) {
    if (!classPolicy.isAllowed(beanClass.getName())) {
      throw new SecurityException(
          "Class " + beanClass.getName() + " is not allowed for recurring job execution.");
    }
    try (BeanResolver.ManagedBeanHandle<?> handle = beanResolver.resolveManaged(beanClass)) {
      handle.get();
    } catch (IllegalStateException e) {
      throw new IllegalStateException(
          "@Recurring method on unresolvable managed bean: " + beanClass.getName(), e);
    }
  }

  @PreDestroy
  void cleanup() {
    methodCache.clear();
  }

  private Method getOrResolveMethod(
      Class<?> beanClass, Class<?> resolvedBeanClass, String methodName, boolean hasJobContextParam)
      throws NoSuchMethodException {
    Class<?> lookupClass =
        beanClass.isAssignableFrom(resolvedBeanClass) ? beanClass : resolvedBeanClass;
    MethodCacheKey key =
        new MethodCacheKey(
            beanClass.getName(), lookupClass.getName(), methodName, hasJobContextParam);

    Method cached = methodCache.get(key);
    if (cached != null) {
      return cached;
    }

    Method resolved =
        findMethod(lookupClass, methodName, hasJobContextParam, lookupClass == beanClass);
    if (resolved == null) {
      Method opposite =
          findMethod(lookupClass, methodName, !hasJobContextParam, lookupClass == beanClass);
      if (opposite != null) {
        throw signatureChanged(beanClass, methodName, hasJobContextParam);
      }
      throw new NoSuchMethodException(
          beanClass.getName() + "." + methodName + (hasJobContextParam ? "(JobContext)" : "()"));
    }

    Method existing = methodCache.putIfAbsent(key, resolved);
    return existing != null ? existing : resolved;
  }

  private static Method findMethod(
      Class<?> lookupClass,
      String methodName,
      boolean hasJobContextParam,
      boolean walkDeclaredHierarchy) {
    Class<?>[] parameterTypes =
        hasJobContextParam ? new Class<?>[] {JobContext.class} : new Class<?>[0];
    if (!walkDeclaredHierarchy) {
      try {
        Method method = lookupClass.getMethod(methodName, parameterTypes);
        return method.isSynthetic() || method.isBridge() ? null : method;
      } catch (NoSuchMethodException e) {
        return null;
      }
    }

    Class<?> current = lookupClass;
    while (current != null && current != Object.class) {
      for (Method method : current.getDeclaredMethods()) {
        if (method.isSynthetic()
            || method.isBridge()
            || !method.getName().equals(methodName)
            || !java.util.Arrays.equals(method.getParameterTypes(), parameterTypes)) {
          continue;
        }
        return method;
      }
      current = current.getSuperclass();
    }
    return null;
  }

  private static NoSuchMethodException signatureChanged(
      Class<?> beanClass, String methodName, boolean hasJobContextParam) {
    String mismatch =
        hasJobContextParam
            ? "Job payload expects (JobContext) parameter but method now has no parameters."
            : "Job payload expects no parameters but method now requires (JobContext).";
    return new NoSuchMethodException(
        "Method signature changed for @Recurring job: "
            + beanClass.getName()
            + "."
            + methodName
            + ". "
            + mismatch
            + " Cancel and re-register the recurring job, or redeploy to pick up the new"
            + " signature.");
  }

  private static void invokeMethod(Method method, Object bean, Object... args) throws Exception {
    try {
      method.invoke(bean, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("Recurring method threw an unrecoverable Throwable", cause);
    }
  }

  private record MethodCacheKey(
      String className, String resolvedClassName, String methodName, boolean hasJobContextParam) {}
}
