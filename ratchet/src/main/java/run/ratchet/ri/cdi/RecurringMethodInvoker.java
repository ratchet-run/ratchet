package run.ratchet.ri.cdi;

import run.ratchet.api.JobContext;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Invokes recurring job methods on their CDI bean instances.
 *
 * <p>This service handles CDI bean instance lookup, early validation of bean resolvability at
 * registration time, and runtime invocation with optional {@link JobContext} parameter. Method
 * objects are cached to avoid repeated reflection lookups.
 *
 * <p>The invoker supports two method signatures:
 *
 * <ul>
 *   <li>No parameters: {@code void myJob()}
 *   <li>JobContext parameter: {@code void myJob(JobContext ctx)}
 * </ul>
 *
 * @see RecurringJobProcessor
 */
@ApplicationScoped
public class RecurringMethodInvoker {

  private record MethodCacheKey(String className, String methodName, boolean hasJobContextParam) {}

  private final ConcurrentMap<MethodCacheKey, Method> methodCache = new ConcurrentHashMap<>();

  @Inject @Any private Instance<Object> allBeans;

  /**
   * Invokes a recurring job method by class and method name.
   *
   * @param beanClassName the fully-qualified class name containing the method
   * @param methodName the name of the method to invoke
   * @param hasJobContextParam true if the method accepts a JobContext parameter
   * @throws Exception if the bean/method cannot be found or invocation fails
   */
  @SuppressWarnings("java:S112")
  public void invoke(String beanClassName, String methodName, boolean hasJobContextParam)
      throws Exception {
    Class<?> beanClass = Class.forName(beanClassName);
    Instance<?> instance = allBeans.select(beanClass);

    if (instance.isUnsatisfied()) {
      throw new IllegalStateException("No CDI bean found for class: " + beanClassName);
    }

    Object bean = instance.get();
    Method method = getOrResolveMethod(beanClass, methodName, hasJobContextParam);

    if (hasJobContextParam) {
      JobContext context = JobContext.current();
      method.invoke(bean, context);
    } else {
      method.invoke(bean);
    }
  }

  /**
   * Validates that a bean class is resolvable via CDI at registration time.
   *
   * @param beanClass the bean class to validate
   * @throws IllegalStateException if the bean cannot be resolved from CDI
   */
  public void validateBeanResolvable(Class<?> beanClass) {
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
      } catch (NoSuchMethodException ignored) {
        throw e;
      }
    }

    methodCache.put(key, resolved);
    return resolved;
  }
}
