package run.ratchet.ri.cdi;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AnnotatedMethod;
import jakarta.enterprise.inject.spi.AnnotatedType;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.ProcessAnnotatedType;
import jakarta.enterprise.inject.spi.WithAnnotations;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import run.ratchet.api.Recurring;

/**
 * Collects CDI bean classes that declare {@link Recurring} methods during type discovery.
 *
 * <p>The startup processor uses this metadata to query the BeanManager for only candidate recurring
 * bean classes instead of enumerating every bean in the deployment.
 *
 * <p>Callers retrieve the discovered set via {@link #getRecurringBeanClasses()} on the Extension
 * instance obtained from the deployment's own {@link jakarta.enterprise.inject.spi.BeanManager}.
 * This avoids the static-singleton pattern that breaks when multiple CDI containers share a JVM
 * (e.g., multiple WildFly deployments), where a second {@code BeforeBeanDiscovery} event would
 * overwrite a shared static field and lose the first deployment's discovered classes.
 *
 * @apiNote Internal RI implementation. The class is public because the CDI {@code
 *     jakarta.enterprise.inject.spi.Extension} ServiceLoader mechanism requires it, and {@link
 *     #getRecurringBeanClasses()} is public because it is called reflectively from {@link
 *     RecurringJobProcessor} after {@code BeanManager.getExtension()} returns this instance.
 *     Applications must not reference this class directly. Not part of the supported API surface.
 */
public class RecurringMethodDiscoveryExtension implements Extension {

  private final Set<Class<?>> recurringBeanClasses = ConcurrentHashMap.newKeySet();

  void clear(@Observes BeforeBeanDiscovery event) {
    recurringBeanClasses.clear();
  }

  <T> void collectRecurringType(
      @Observes @WithAnnotations(Recurring.class) ProcessAnnotatedType<T> event) {
    AnnotatedType<T> annotatedType = event.getAnnotatedType();
    if (hasRecurringMethod(annotatedType)) {
      recurringBeanClasses.add(annotatedType.getJavaClass());
    }
  }

  /**
   * Returns the set of bean classes that declare at least one {@link Recurring} method, as
   * discovered for this CDI container. Obtain this Extension instance from the deployment's own
   * {@link jakarta.enterprise.inject.spi.BeanManager} to ensure deployment-scoped isolation.
   */
  public Set<Class<?>> getRecurringBeanClasses() {
    return Set.copyOf(recurringBeanClasses);
  }

  private static boolean hasRecurringMethod(AnnotatedType<?> annotatedType) {
    for (AnnotatedMethod<?> method : annotatedType.getMethods()) {
      if (method.isAnnotationPresent(Recurring.class)) {
        return true;
      }
    }
    return false;
  }
}
