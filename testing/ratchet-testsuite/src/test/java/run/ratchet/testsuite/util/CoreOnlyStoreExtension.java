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
package run.ratchet.testsuite.util;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.BeanAttributes;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.ProcessBeanAttributes;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.Set;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Test CDI extension that demotes the deployed store bean to the mandatory core contract.
 *
 * <p>It rewrites the store bean's set of bean types to exclude every optional capability interface
 * (and any composite type that re-introduces one), keeping only {@link JobStore} and its core
 * sub-interfaces. With the capabilities gone from the bean's type set, {@code @Inject
 * Instance<Cap>} resolves to <em>unsatisfied</em> in a real container.
 *
 * <p>The deployment therefore only boots if the reference engine treats every capability as
 * optional. If any capability had stayed a hard {@code @Inject}, the container would refuse the
 * deployment with an unsatisfied-dependency error — which is exactly the leak this proves absent.
 */
public class CoreOnlyStoreExtension implements Extension {

  private static final Set<Class<?>> OPTIONAL_CAPABILITIES =
      Set.of(
          RecurringJobStore.class,
          BatchStore.class,
          WorkflowConditionStore.class,
          SignalStore.class,
          ResourcePermitStore.class,
          LockStore.class,
          ArchiveStore.class,
          JobQueryStore.class,
          JobAnalyticsStore.class,
          JobAuditStore.class,
          DlqAlertStore.class,
          JobExtensionStore.class);

  <T> void demoteStoreToCore(@Observes ProcessBeanAttributes<T> event) {
    BeanAttributes<T> attributes = event.getBeanAttributes();
    Set<Type> beanTypes = attributes.getTypes();

    // Only the store bean carries both JobStore and at least one optional capability.
    boolean isStore = beanTypes.stream().anyMatch(t -> rawType(t) == JobStore.class);
    boolean advertisesCapability = beanTypes.stream().anyMatch(t -> isCapability(rawType(t)));
    if (!isStore || !advertisesCapability) {
      return;
    }

    Set<Type> coreTypes = new LinkedHashSet<>();
    for (Type type : beanTypes) {
      Class<?> raw = rawType(type);
      // Drop the capability interfaces and the per-store composite interface that extends them, so
      // the bean can no longer satisfy Instance<Cap>. Keep the concrete impl class: CDI matches a
      // raw required type against a bean's declared types by identity, so retaining the class does
      // not re-introduce a capability as an injectable type (the @Typed rule). Dropping the class
      // forced Weld to generate the bean's client proxy in the SPI interface package, where it
      // could not subclass the package-private impl on a strict proxy loader (OpenLiberty) — the
      // whole deployment failed with WELD-001524 / IllegalAccessError. Keeping the class anchors
      // the proxy in the impl's own package, where the package-private access is legal.
      if (raw == null || isCapability(raw) || (raw.isInterface() && implementsCapability(raw))) {
        continue;
      }
      coreTypes.add(type);
    }
    event.configureBeanAttributes().types(coreTypes);
  }

  private static boolean isCapability(Class<?> raw) {
    return raw != null && OPTIONAL_CAPABILITIES.contains(raw);
  }

  private static boolean implementsCapability(Class<?> raw) {
    return OPTIONAL_CAPABILITIES.stream().anyMatch(cap -> cap.isAssignableFrom(raw));
  }

  private static Class<?> rawType(Type type) {
    if (type instanceof Class<?> c) {
      return c;
    }
    if (type instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> c) {
      return c;
    }
    return null;
  }
}
