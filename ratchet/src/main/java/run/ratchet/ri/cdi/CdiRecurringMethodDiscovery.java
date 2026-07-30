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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;

/**
 * CDI adapter that turns deployment-scoped extension metadata into stable recurring bean classes.
 *
 * <p>When extension metadata is unavailable or empty, the adapter preserves the historical
 * compatibility fallback of scanning every CDI bean class.
 */
@ApplicationScoped
public class CdiRecurringMethodDiscovery implements RecurringMethodDiscovery {

  private final BeanManager beanManager;

  protected CdiRecurringMethodDiscovery() {
    this.beanManager = null;
  }

  @Inject
  public CdiRecurringMethodDiscovery(BeanManager beanManager) {
    this.beanManager = beanManager;
  }

  @Override
  public Set<Class<?>> recurringBeanClasses() {
    Set<Class<?>> candidates = extensionCandidates();
    Set<Bean<?>> beans = new LinkedHashSet<>();
    if (candidates.isEmpty()) {
      beans.addAll(beanManager.getBeans(Object.class, Any.Literal.INSTANCE));
    } else {
      for (Class<?> candidate : candidates) {
        beans.addAll(beanManager.getBeans(candidate, Any.Literal.INSTANCE));
      }
    }

    Set<Class<?>> beanClasses = new LinkedHashSet<>();
    for (Bean<?> bean : beans) {
      Class<?> beanClass = bean.getBeanClass();
      if (beanClass != null && !beanClass.isSynthetic() && !Proxy.isProxyClass(beanClass)) {
        beanClasses.add(beanClass);
      }
    }
    return Collections.unmodifiableSet(beanClasses);
  }

  private Set<Class<?>> extensionCandidates() {
    try {
      RecurringMethodDiscoveryExtension extension =
          beanManager.getExtension(RecurringMethodDiscoveryExtension.class);
      return extension.getRecurringBeanClasses();
    } catch (RuntimeException exception) {
      return Set.of();
    }
  }
}
