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
 */
public class RecurringMethodDiscoveryExtension implements Extension {

  private static final Set<Class<?>> RECURRING_BEAN_CLASSES = ConcurrentHashMap.newKeySet();

  void clear(@Observes BeforeBeanDiscovery event) {
    RECURRING_BEAN_CLASSES.clear();
  }

  <T> void collectRecurringType(
      @Observes @WithAnnotations(Recurring.class) ProcessAnnotatedType<T> event) {
    AnnotatedType<T> annotatedType = event.getAnnotatedType();
    if (hasRecurringMethod(annotatedType)) {
      RECURRING_BEAN_CLASSES.add(annotatedType.getJavaClass());
    }
  }

  static Set<Class<?>> recurringBeanClasses() {
    return Set.copyOf(RECURRING_BEAN_CLASSES);
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
