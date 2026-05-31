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
