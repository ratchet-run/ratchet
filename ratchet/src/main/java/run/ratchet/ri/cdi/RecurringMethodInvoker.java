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
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import run.ratchet.api.JobContext;
import run.ratchet.spi.ClassPolicy;

/** Invokes @Recurring methods on their CDI beans. */
@ApplicationScoped
public class RecurringMethodInvoker {

  private final ConcurrentMap<MethodCacheKey, Method> methodCache = new ConcurrentHashMap<>();
  private final Instance<Object> allBeans;
  private final ClassPolicy classPolicy;

  protected RecurringMethodInvoker() {
    this.allBeans = null;
    this.classPolicy = null;
  }

  @Inject
  public RecurringMethodInvoker(@Any Instance<Object> allBeans, ClassPolicy classPolicy) {
    this.allBeans = allBeans;
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
    Instance<?> instance = allBeans.select(beanClass);

    if (instance.isUnsatisfied()) {
      throw new IllegalStateException("No CDI bean found for class: " + beanClassName);
    }

    Instance.Handle<?> handle = instance.getHandle();
    Object bean = handle.get();
    try {
      Method method = getOrResolveMethod(beanClass, methodName, hasJobContextParam);

      if (hasJobContextParam) {
        JobContext context = JobContext.current();
        invokeMethod(method, bean, context);
      } else {
        invokeMethod(method, bean);
      }
    } finally {
      if (handle.getBean().getScope().equals(Dependent.class)) {
        handle.destroy();
      }
    }
  }

  public void validateBeanResolvable(Class<?> beanClass) {
    if (!classPolicy.isAllowed(beanClass.getName())) {
      throw new SecurityException(
          "Class " + beanClass.getName() + " is not allowed for recurring job execution.");
    }
    Instance<?> instance = allBeans.select(beanClass);
    if (instance.isUnsatisfied()) {
      throw new IllegalStateException(
          "@Recurring method on unresolvable CDI bean: " + beanClass.getName());
    }
  }

  @PreDestroy
  void cleanup() {
    methodCache.clear();
  }

  private Method getOrResolveMethod(
      Class<?> beanClass, String methodName, boolean hasJobContextParam)
      throws NoSuchMethodException {
    MethodCacheKey key = new MethodCacheKey(beanClass.getName(), methodName, hasJobContextParam);

    Method cached = methodCache.get(key);
    if (cached != null) {
      return cached;
    }

    Method resolved;
    try {
      if (hasJobContextParam) {
        resolved = beanClass.getDeclaredMethod(methodName, JobContext.class);
      } else {
        resolved = beanClass.getDeclaredMethod(methodName);
      }
    } catch (NoSuchMethodException e) {
      // Check if the method exists with the opposite signature (signature change detection)
      try {
        if (hasJobContextParam) {
          beanClass.getDeclaredMethod(methodName);
          throw new NoSuchMethodException(
              "Method signature changed for @Recurring job: "
                  + beanClass.getName()
                  + "."
                  + methodName
                  + ". Job payload expects (JobContext) parameter but method now has no parameters."
                  + " Cancel and re-register the recurring job, or redeploy to pick up the new"
                  + " signature.");
        } else {
          beanClass.getDeclaredMethod(methodName, JobContext.class);
          throw new NoSuchMethodException(
              "Method signature changed for @Recurring job: "
                  + beanClass.getName()
                  + "."
                  + methodName
                  + ". Job payload expects no parameters but method now requires (JobContext)."
                  + " Cancel and re-register the recurring job, or redeploy to pick up the new"
                  + " signature.");
        }
      } catch (NoSuchMethodException signatureCheckFailure) {
        // Method was not found under either supported signature; rethrow the original miss.
        throw e;
      }
    }

    methodCache.put(key, resolved);
    return resolved;
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

  private record MethodCacheKey(String className, String methodName, boolean hasJobContextParam) {}
}
