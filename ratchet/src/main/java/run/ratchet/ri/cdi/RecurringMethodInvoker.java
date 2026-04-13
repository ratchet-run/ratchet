package run.ratchet.ri.cdi;

import run.ratchet.api.JobContext;
import run.ratchet.spi.ClassPolicy;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Invokes @Recurring methods on their CDI beans. */
@ApplicationScoped
public class RecurringMethodInvoker {

  private record MethodCacheKey(String className, String methodName, boolean hasJobContextParam) {}

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

    Object bean = instance.get();
    Method method = getOrResolveMethod(beanClass, methodName, hasJobContextParam);

    if (hasJobContextParam) {
      JobContext context = JobContext.current();
      method.invoke(bean, context);
    } else {
      method.invoke(bean);
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
      } catch (NoSuchMethodException ignored) {
        throw e;
      }
    }

    methodCache.put(key, resolved);
    return resolved;
  }
}
